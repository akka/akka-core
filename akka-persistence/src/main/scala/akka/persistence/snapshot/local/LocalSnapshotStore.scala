/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.snapshot.local

import java.io._
import java.net.{ URLDecoder, URLEncoder }
import java.nio.file.Files
import java.nio.file.NoSuchFileException

import scala.collection.immutable
import scala.concurrent.Future
import scala.util._

import com.typesafe.config.Config

import akka.actor.ActorLogging
import akka.persistence._
import akka.persistence.serialization._
import akka.persistence.snapshot._
import akka.serialization.SerializationExtension
import akka.util.ByteString.UTF_8

/**
 * INTERNAL API
 *
 * Local filesystem backed snapshot store.
 */
private[persistence] class LocalSnapshotStore(config: Config) extends SnapshotStore with ActorLogging {
  private val FilenamePattern = """^snapshot-(.+)-(\d+)-(\d+)""".r
  private val persistenceIdStartIdx = 9 // Persistence ID starts after the "snapshot-" substring

  import akka.util.Helpers._
  private val maxLoadAttempts = config.getInt("max-load-attempts").requiring(_ > 1, "max-load-attempts must be >= 1")

  private val streamDispatcher = context.system.dispatchers.lookup(config.getString("stream-dispatcher"))
  private val dir = new File(config.getString("dir"))

  private val serializationExtension = SerializationExtension(context.system)
  private var saving = immutable.Set.empty[SnapshotMetadata] // saving in progress

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    //
    // Heuristics:
    //
    // Select youngest `maxLoadAttempts` snapshots that match upper bound.
    // This may help in situations where saving of a snapshot could not be completed because of a JVM crash.
    // Hence, an attempt to load that snapshot will fail but loading an older snapshot may succeed.
    //
    val metadata = snapshotMetadatas(persistenceId, criteria).sorted.takeRight(maxLoadAttempts)
    Future {
      load(metadata) match {
        case Success(s) => s
        case Failure(e) => throw e // all attempts failed, fail the future
      }
    }(streamDispatcher).recoverWith {
      // retry if we listed an older snapshot that was deleted before loading
      case _: NoSuchFileException => loadAsync(persistenceId, criteria)
    }(streamDispatcher)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    saving += metadata
    val completion = Future(save(metadata, snapshot))(streamDispatcher)
    completion
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    saving -= metadata
    Future {
      // multiple snapshot files here mean that there were multiple snapshots for this seqNr, we delete all of them
      // usually snapshot-stores would keep one snapshot per sequenceNr however here in the file-based one we timestamp
      // snapshots and allow multiple to be kept around (for the same seqNr) if desired
      snapshotFiles(metadata).map(_.delete())
    }(streamDispatcher).map(_ => ())(streamDispatcher)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val metadatas = snapshotMetadatas(persistenceId, criteria)
    Future
      .sequence {
        metadatas.map(deleteAsync)
      }(scala.collection.immutable.IndexedSeq, streamDispatcher)
      .map(_ => ())(streamDispatcher)
  }

  override def receivePluginInternal: Receive = {
    case SaveSnapshotSuccess(metadata) => saving -= metadata
    case _: SaveSnapshotFailure        => // ignore
    case _: DeleteSnapshotsSuccess     => // ignore
    case _: DeleteSnapshotsFailure     => // ignore
  }

  private def snapshotFiles(metadata: SnapshotMetadata): immutable.Seq[File] = {
    snapshotDir().listFiles(new SnapshotSeqNrFilenameFilter(metadata)).toVector
  }

  @scala.annotation.tailrec
  private def load(metadata: immutable.Seq[SnapshotMetadata]): Try[Option[SelectedSnapshot]] =
    metadata.lastOption match {
      case None => Success(None) // no snapshots stored
      case Some(md) =>
        Try(withInputStream(md)(deserialize)) match {
          case Success(s) =>
            Success(Some(SelectedSnapshot(md, s.data)))
          case Failure(e) =>
            val remaining = metadata.init
            log.error(e, s"Error loading snapshot [{}], remaining attempts: [{}]", md, remaining.size)
            if (remaining.isEmpty)
              Failure(e) // all attempts failed
            else
              load(remaining) // try older snapshot
        }
    }

  protected def save(metadata: SnapshotMetadata, snapshot: Any): Unit = {
    val tmpFile = withOutputStream(metadata)(serialize(_, Snapshot(snapshot)))
    tmpFile.renameTo(snapshotFileForWrite(metadata))
  }

  protected def deserialize(inputStream: InputStream): Snapshot =
    serializationExtension.deserialize(streamToBytes(inputStream), classOf[Snapshot]).get

  protected def serialize(outputStream: OutputStream, snapshot: Snapshot): Unit = {
    outputStream.write(serializationExtension.serialize(snapshot).get)
  }

  protected def withOutputStream(metadata: SnapshotMetadata)(p: (OutputStream) => Unit): File = {
    val tmpFile = snapshotFileForWrite(metadata, extension = "tmp")
    withStream(new BufferedOutputStream(Files.newOutputStream(tmpFile.toPath())), p)
    tmpFile
  }

  private def withInputStream[T](metadata: SnapshotMetadata)(p: (InputStream) => T): T =
    withStream(new BufferedInputStream(Files.newInputStream(snapshotFileForWrite(metadata).toPath())), p)

  private def withStream[A <: Closeable, B](stream: A, p: A => B): B =
    try {
      p(stream)
    } finally {
      stream.close()
    }

  /** Only by persistenceId and sequenceNr, timestamp is informational - accommodates for 2.13.x series files */
  protected def snapshotFileForWrite(metadata: SnapshotMetadata, extension: String = ""): File =
    new File(
      snapshotDir(),
      s"snapshot-${URLEncoder.encode(metadata.persistenceId, UTF_8)}-${metadata.sequenceNr}-${metadata.timestamp}${extension}")

  private def snapshotMetadatas(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): immutable.Seq[SnapshotMetadata] = {
    val files = snapshotDir().listFiles(new SnapshotFilenameFilter(persistenceId))
    if (files eq null) Nil // if the dir was removed
    else {
      files
        .map(_.getName)
        .flatMap { filename =>
          extractMetadata(filename).map {
            case (pid, snr, tms) => SnapshotMetadata(URLDecoder.decode(pid, UTF_8), snr, tms)
          }
        }
        .filter(md => criteria.matches(md) && !saving.contains(md))
        .toVector
    }
  }

  override def preStart(): Unit = {
    snapshotDir()
    super.preStart()
  }

  private def snapshotDir(): File = {
    if (!dir.isDirectory) {
      // try to create the directory, on failure double check if someone else beat us to it
      if (!dir.mkdirs() && !dir.isDirectory) {
        throw new IOException(s"Failed to create snapshot directory [${dir.getCanonicalPath}]")
      }
    }
    dir
  }

  // system default encoding is a bad idea but kept for backwards compatibility
  private val defaultSystemEncoding = System.getProperty("file.encoding")

  private final class SnapshotFilenameFilter(persistenceId: String) extends FilenameFilter {
    val encodedPersistenceId = URLEncoder.encode(persistenceId, defaultSystemEncoding)

    def accept(dir: File, name: String): Boolean = {
      val persistenceIdEndIdx = name.lastIndexOf('-', name.lastIndexOf('-') - 1)
      persistenceIdStartIdx + encodedPersistenceId.length == persistenceIdEndIdx &&
      name.startsWith(encodedPersistenceId, persistenceIdStartIdx)
    }
  }

  private final class SnapshotSeqNrFilenameFilter(md: SnapshotMetadata) extends FilenameFilter {
    private final def matches(pid: String, snr: String, tms: String): Boolean = {
      pid.equals(URLEncoder.encode(md.persistenceId, defaultSystemEncoding)) &&
      Try(snr.toLong == md.sequenceNr && (md.timestamp == 0L || tms.toLong == md.timestamp)).getOrElse(false)
    }

    def accept(dir: File, name: String): Boolean =
      name match {
        case FilenamePattern(pid, snr, tms) => matches(pid, snr, tms)
        case _                              => false
      }

  }

  private def extractMetadata(filename: String): Option[(String, Long, Long)] = {
    val sequenceNumberEndIdx = filename.lastIndexOf('-')
    val persistenceIdEndIdx = filename.lastIndexOf('-', sequenceNumberEndIdx - 1)
    val timestampString = filename.substring(sequenceNumberEndIdx + 1)
    if (persistenceIdStartIdx >= persistenceIdEndIdx || timestampString.exists(!_.isDigit)) None
    else {
      val persistenceId = filename.substring(persistenceIdStartIdx, persistenceIdEndIdx)
      val sequenceNumber = filename.substring(persistenceIdEndIdx + 1, sequenceNumberEndIdx).toLong
      val timestamp = filename.substring(sequenceNumberEndIdx + 1).toLong
      Some((persistenceId, sequenceNumber, timestamp))
    }
  }
}
