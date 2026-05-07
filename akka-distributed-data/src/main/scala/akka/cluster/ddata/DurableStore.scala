/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

import com.typesafe.config.Config
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import org.lmdbjava.Txn

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.DeadLetterSuppression
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.Replicator.ReplicatorMessage
import akka.io.DirectByteBufferPool
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.util.ByteString
import akka.util.OptionVal
import scala.jdk.CollectionConverters._

/**
 * An actor implementing the durable store for the Distributed Data `Replicator`
 * has to implement the protocol with the messages defined here.
 *
 * At startup the `Replicator` creates the durable store actor and sends the
 * `Load` message to it. It must then reply with 0 or more `LoadData` messages
 * followed by one `LoadAllCompleted` message to the `sender` (the `Replicator`).
 *
 * If the `LoadAll` fails it can throw `LoadFailed` and the `Replicator` supervisor
 * will stop itself and the durable store.
 *
 * When the `Replicator` needs to store a value it sends a `Store` message
 * to the durable store actor, which must then reply with the `successMsg` or
 * `failureMsg` to the `replyTo`.
 *
 * When entries have expired the `Replicator` sends a `Expire` message to the durable
 * store actor, which can delete the entries from the backend store.
 */
object DurableStore {

  /**
   * Request to store an entry. It optionally contains a `StoreReply`, which
   * should be used to signal success or failure of the operation to the contained
   * `replyTo` actor.
   */
  final case class Store(key: KeyId, data: DurableDataEnvelope, reply: Option[StoreReply])
  final case class StoreReply(successMsg: Any, failureMsg: Any, replyTo: ActorRef)

  /**
   * Request to load all entries.
   *
   * It must reply with 0 or more `LoadData` messages
   * followed by one `LoadAllCompleted` message to the `sender` (the `Replicator`).
   *
   * If the `LoadAll` fails it can throw `LoadFailed` and the `Replicator` supervisor
   * will stop itself and the durable store.
   */
  case object LoadAll
  final case class LoadData(data: Map[KeyId, DurableDataEnvelope])
  case object LoadAllCompleted
  class LoadFailed(message: String, cause: Throwable) extends RuntimeException(message, cause) {
    def this(message: String) = this(message, null)
  }

  /**
   * Request to expire (remove) entries.
   */
  final case class Expire(keys: Set[KeyId])

  /**
   * Wrapper class for serialization of a data value.
   * The `ReplicatorMessageSerializer` will serialize/deserialize
   * the wrapped `ReplicatedData` including its serializerId and
   * manifest.
   */
  final class DurableDataEnvelope private[akka] (private[akka] val dataEnvelope: DataEnvelope)
      extends ReplicatorMessage {

    def this(data: ReplicatedData) = this(DataEnvelope(data))

    def data: ReplicatedData = dataEnvelope.data

    override def toString(): String = s"DurableDataEnvelope($data)"
    override def hashCode(): Int = data.hashCode
    override def equals(o: Any): Boolean = o match {
      case other: DurableDataEnvelope => data == other.data
      case _                          => false
    }
  }
}

object LmdbDurableStore {
  def props(config: Config): Props =
    Props(new LmdbDurableStore(config))

  private case object WriteBehind extends DeadLetterSuppression

  private final case class Lmdb(
      env: Env[ByteBuffer],
      db: Dbi[ByteBuffer],
      keyBuffer: ByteBuffer,
      valueBuffer: ByteBuffer)
}

final class LmdbDurableStore(config: Config) extends Actor with ActorLogging {
  import DurableStore._
  import LmdbDurableStore.Lmdb
  import LmdbDurableStore.WriteBehind

  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[DurableDataEnvelope]).asInstanceOf[SerializerWithStringManifest]
  val manifest = serializer.manifest(new DurableDataEnvelope(Replicator.Internal.DeletedData))

  val writeBehindInterval = config.getString("lmdb.write-behind-interval").toLowerCase match {
    case "off" => Duration.Zero
    case _     => config.getDuration("lmdb.write-behind-interval", MILLISECONDS).millis
  }

  val dir = config.getString("lmdb.dir") match {
    case path if path.endsWith("ddata") =>
      new File(s"$path-${context.system.name}-${self.path.parent.name}-${Cluster(context.system).selfAddress.port.get}")
    case path =>
      new File(path)
  }

  // lazy init
  private var _lmdb: OptionVal[Lmdb] = OptionVal.None

  private def lmdb(): Lmdb = _lmdb match {
    case OptionVal.Some(l) => l
    case _ =>
      val debugEnabled = log.isDebugEnabled
      val t0 = if (debugEnabled) System.nanoTime() else 0L
      log.info("Using durable data in LMDB directory [{}]", dir.getCanonicalPath)
      val env = {
        val mapSize = config.getBytes("lmdb.map-size")
        dir.mkdirs()
        Env.create().setMapSize(mapSize).setMaxDbs(1).open(dir, EnvFlags.MDB_NOLOCK)
      }

      val db = env.openDbi("ddata", DbiFlags.MDB_CREATE)

      val keyBuffer = ByteBuffer.allocateDirect(env.getMaxKeySize)
      val valueBuffer = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

      if (debugEnabled)
        log.debug(
          "Init of LMDB in directory [{}] took [{} ms]",
          dir.getCanonicalPath,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))
      val l = Lmdb(env, db, keyBuffer, valueBuffer)
      _lmdb = OptionVal.Some(l)
      l
  }

  def isDbInitialized: Boolean = _lmdb.isDefined

  def ensureValueBufferSize(size: Int): Unit = {
    val valueBuffer = lmdb().valueBuffer
    if (valueBuffer.remaining < size) {
      DirectByteBufferPool.tryCleanDirectByteBuffer(valueBuffer)
      _lmdb = OptionVal.Some(lmdb().copy(valueBuffer = ByteBuffer.allocateDirect(size * 2)))
    }
  }

  // pending write behind
  val pending = new java.util.HashMap[KeyId, DurableDataEnvelope]

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    // Load is only done on first start, not on restart
    context.become(active)
  }

  override def postStop(): Unit = {
    super.postStop()
    writeBehind()
    if (isDbInitialized) {
      val l = lmdb()
      Try(l.db.close())
      Try(l.env.close())
      DirectByteBufferPool.tryCleanDirectByteBuffer(l.keyBuffer)
      DirectByteBufferPool.tryCleanDirectByteBuffer(l.valueBuffer)
    }
  }

  def receive: Receive = init

  def init: Receive = {
    case LoadAll =>
      if (dir.exists && dir.list().length > 0) {
        val debugEnabled = log.isDebugEnabled
        val t0 = if (debugEnabled) System.nanoTime() else 0L
        val l = lmdb()
        val tx = l.env.txnRead()
        try {
          val iter = l.db.iterate(tx)
          try {
            var n = 0
            val loadData = LoadData(iter.asScala.map { entry =>
              n += 1
              val keyArray = new Array[Byte](entry.key.remaining)
              entry.key.get(keyArray)
              val key = new String(keyArray, ByteString.UTF_8)
              val valArray = new Array[Byte](entry.`val`.remaining)
              entry.`val`.get(valArray)
              val envelope = serializer.fromBinary(valArray, manifest).asInstanceOf[DurableDataEnvelope]
              key -> envelope
            }.toMap)
            if (loadData.data.nonEmpty)
              sender() ! loadData
            sender() ! LoadAllCompleted
            if (debugEnabled)
              log.debug(
                "load all of [{}] entries took [{} ms]",
                n,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))
            context.become(active)
          } finally {
            Try(iter.close())
          }
        } catch {
          case NonFatal(e) =>
            throw new LoadFailed("failed to load durable distributed-data", e)
        } finally {
          Try(tx.close())
        }
      } else {
        // no files to load
        sender() ! LoadAllCompleted
        context.become(active)
      }
  }

  def active: Receive = {
    case Store(key, data, reply) =>
      try {
        lmdb() // init
        if (writeBehindInterval.length == 0) {
          dbPut(OptionVal.None, key, data)
        } else {
          if (pending.isEmpty)
            context.system.scheduler.scheduleOnce(writeBehindInterval, self, WriteBehind)(context.dispatcher)
          pending.put(key, data)
        }
        reply match {
          case Some(StoreReply(successMsg, _, replyTo)) =>
            replyTo ! successMsg
          case None =>
        }
      } catch {
        case NonFatal(e) =>
          log.error(e, "failed to store [{}]", key)
          reply match {
            case Some(StoreReply(_, failureMsg, replyTo)) =>
              replyTo ! failureMsg
            case None =>
          }
      }

    case WriteBehind =>
      writeBehind()

    case Expire(keys) =>
      dbDelete(keys)
  }

  def dbPut(tx: OptionVal[Txn[ByteBuffer]], key: KeyId, data: DurableDataEnvelope): Unit = {
    try {
      val value = serializer.toBinary(data)
      ensureValueBufferSize(value.length)
      val l = lmdb()
      l.keyBuffer.put(key.getBytes(ByteString.UTF_8)).flip()
      l.valueBuffer.put(value).flip()
      tx match {
        case OptionVal.Some(t) => l.db.put(t, l.keyBuffer, l.valueBuffer)
        case _                 => l.db.put(l.keyBuffer, l.valueBuffer)
      }
    } finally {
      val l = lmdb()
      l.keyBuffer.clear()
      l.valueBuffer.clear()
    }
  }

  def writeBehind(): Unit = {
    if (!pending.isEmpty()) {
      val debugEnabled = log.isDebugEnabled
      val t0 = if (debugEnabled) System.nanoTime() else 0L
      val tx = lmdb().env.txnWrite()
      try {
        val iter = pending.entrySet.iterator
        while (iter.hasNext) {
          val entry = iter.next()
          dbPut(OptionVal.Some(tx), entry.getKey, entry.getValue)
        }
        tx.commit()
        if (debugEnabled)
          log.debug(
            "store and commit of [{}] entries took [{} ms]",
            pending.size,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))
      } catch {
        case NonFatal(e) =>
          import scala.jdk.CollectionConverters._
          log.error(e, "failed to store [{}]", pending.keySet.asScala.mkString(","))
          tx.abort()
      } finally {
        pending.clear()
      }
    }
  }

  def dbDelete(keys: Set[KeyId]): Unit = {
    val debugEnabled = log.isDebugEnabled
    val t0 = if (debugEnabled) System.nanoTime() else 0L
    val l = lmdb()
    val tx = lmdb().env.txnWrite()
    try {
      keys.foreach { key =>
        l.keyBuffer.put(key.getBytes(ByteString.UTF_8)).flip()
        l.db.delete(tx, l.keyBuffer)
      }
      tx.commit()
      if (debugEnabled)
        log.debug(
          "delete and commit of [{}] entries took [{} ms]",
          keys.size,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))
    } catch {
      case NonFatal(e) =>
        import scala.jdk.CollectionConverters._
        log.error(e, "failed to delete [{}]", pending.keySet.asScala.mkString(","))
        tx.abort()
    } finally {
      l.keyBuffer.clear()
    }
  }

}
