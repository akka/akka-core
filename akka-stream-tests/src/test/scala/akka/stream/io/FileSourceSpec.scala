/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.util.Random

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration._

import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs

import akka.stream._
import akka.stream.IOResult._
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.impl.StreamSupervisor
import akka.stream.impl.StreamSupervisor.Children
import akka.stream.io.FileSourceSpec.Settings
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

object FileSourceSpec {
  final case class Settings(chunkSize: Int, readAhead: Int)
}

@nowarn
class FileSourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  val fs = Jimfs.newFileSystem("FileSourceSpec", Configuration.unix())

  val TestText = {
    ("a" * 1000) +
    ("b" * 1000) +
    ("c" * 1000) +
    ("d" * 1000) +
    ("e" * 1000) +
    ("f" * 1000)
  }

  val testFile = {
    val f = Files.createTempFile(fs.getPath("/"), "file-source-spec", ".tmp")
    Files.newBufferedWriter(f, UTF_8).append(TestText).close()
    f
  }

  val notExistingFile = {
    // this way we make sure it doesn't accidentally exist
    val f = Files.createTempFile(fs.getPath("/"), "not-existing-file", ".tmp")
    Files.delete(f)
    f
  }

  val directoryInsteadOfFile = Files.createTempDirectory(fs.getPath("/"), "directory-instead-of-file")

  val LinesCount = 2000 + new Random().nextInt(300)

  val manyLines = {
    val f = Files.createTempFile(fs.getPath("/"), s"file-source-spec-lines_$LinesCount", "tmp")
    val w = Files.newBufferedWriter(f, UTF_8)
    (1 to LinesCount).foreach { l =>
      w.append("a" * l).append("\n")
    }
    w.close()
    f
  }

  "FileSource" must {
    "read contents from a file" in {
      val chunkSize = 512

      val p = FileIO
        .fromPath(testFile, chunkSize)
        .addAttributes(Attributes.inputBuffer(1, 2))
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[ByteString]()
      p.subscribe(c)
      val sub = c.expectSubscription()

      var remaining = TestText

      def nextChunk() = {
        val (chunk, rest) = remaining.splitAt(chunkSize)
        remaining = rest
        chunk
      }

      sub.request(1)
      c.expectNext().utf8String should ===(nextChunk().toString)
      sub.request(1)
      c.expectNext().utf8String should ===(nextChunk().toString)
      c.expectNoMessage(300.millis)

      sub.request(200)
      var expectedChunk = nextChunk().toString
      while (expectedChunk != "") {
        c.expectNext().utf8String should ===(expectedChunk)
        expectedChunk = nextChunk().toString
      }
      sub.request(1)

      c.expectComplete()
    }

    "complete future even when abrupt termination happened" in {
      val chunkSize = 512
      val mat = ActorMaterializer()
      val (future, p) = FileIO
        .fromPath(testFile, chunkSize)
        .addAttributes(Attributes.inputBuffer(1, 2))
        .toMat(TestSink())(Keep.both)
        .run()(mat)
      p.request(1)
      p.expectNext().utf8String should ===(TestText.splitAt(chunkSize)._1)
      mat.shutdown()
      future.futureValue === createSuccessful(chunkSize)
    }

    "read partial contents from a file" in {
      val chunkSize = 512
      val startPosition = 1000
      val bufferAttributes = Attributes.inputBuffer(1, 2)

      val p = FileIO
        .fromPath(testFile, chunkSize, startPosition)
        .withAttributes(bufferAttributes)
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[ByteString]()
      p.subscribe(c)
      val sub = c.expectSubscription()
      var remaining = TestText.drop(1000)

      def nextChunk() = {
        val (chunk, rest) = remaining.splitAt(chunkSize)
        remaining = rest
        chunk
      }

      sub.request(5000)

      for (_ <- 1 to 10) {
        c.expectNext().utf8String should ===(nextChunk().toString)
      }
      c.expectComplete()
    }

    "be able to read not whole file" in {
      val chunkSize = 512
      val (future, p) = FileIO
        .fromPath(testFile, chunkSize)
        .addAttributes(Attributes.inputBuffer(1, 2))
        .toMat(TestSink())(Keep.both)
        .run()
      p.request(1)
      p.expectNext().utf8String should ===(TestText.splitAt(chunkSize)._1)
      p.cancel()
      future.futureValue === createSuccessful(chunkSize)
    }

    "complete only when all contents of a file have been signalled" in {
      val chunkSize = 256

      val demandAllButOneChunks = TestText.length / chunkSize - 1

      val p = FileIO
        .fromPath(testFile, chunkSize)
        .addAttributes(Attributes.inputBuffer(4, 8))
        .runWith(Sink.asPublisher(false))

      val c = TestSubscriber.manualProbe[ByteString]()
      p.subscribe(c)
      val sub = c.expectSubscription()

      var remaining = TestText
      def nextChunk() = {
        val (chunk, rest) = remaining.splitAt(chunkSize)
        remaining = rest
        chunk
      }

      sub.request(demandAllButOneChunks)
      for (_ <- 1 to demandAllButOneChunks) c.expectNext().utf8String should ===(nextChunk())
      c.expectNoMessage(300.millis)

      sub.request(1)
      c.expectNext().utf8String should ===(nextChunk())
      c.expectNoMessage(200.millis)

      sub.request(1)
      c.expectNext().utf8String should ===(nextChunk())
      c.expectComplete()
    }

    "onError with failure and return a failed IOResult when trying to read from file which does not exist" in {
      val (r, p) = FileIO.fromPath(notExistingFile).toMat(Sink.asPublisher(false))(Keep.both).run()
      val c = TestSubscriber.manualProbe[ByteString]()
      p.subscribe(c)

      c.expectSubscription()

      val error = c.expectError()
      error shouldBe a[NoSuchFileException]

      r.futureValue.status.isFailure shouldBe true
    }

    "onError with failure and return a failed IOResult when trying to read from a directory instead of a file" in {
      val (r, p) = FileIO.fromPath(directoryInsteadOfFile).toMat(Sink.asPublisher(false))(Keep.both).run()
      val c = TestSubscriber.manualProbe[ByteString]()
      p.subscribe(c)

      c.expectSubscription()

      val error = c.expectError()
      error shouldBe an[IllegalArgumentException]

      r.futureValue.status.isFailure shouldBe true
    }

    List(
      Settings(chunkSize = 512, readAhead = 2),
      Settings(chunkSize = 512, readAhead = 4),
      Settings(chunkSize = 2048, readAhead = 2),
      Settings(chunkSize = 2048, readAhead = 4)).foreach { settings =>
      import settings._

      s"count lines in real file (chunkSize = $chunkSize, readAhead = $readAhead)" in {
        val s =
          FileIO.fromPath(manyLines, chunkSize = chunkSize).withAttributes(Attributes.inputBuffer(readAhead, readAhead))

        val f = s.runWith(Sink.fold(0) { case (acc, l) => acc + l.utf8String.count(_ == '\n') })

        f.futureValue should ===(LinesCount)
      }
    }

    "complete materialized future with a failure if upstream fails" in {
      val matVal: Future[IOResult] =
        FileIO.fromPath(manyLines, chunkSize = 4).map(_ => throw new RuntimeException).to(Sink.ignore).run()

      matVal.failed.futureValue shouldBe a[IOOperationIncompleteException]
    }

    "use dedicated blocking-io-dispatcher by default" in {
      val p = FileIO.fromPath(manyLines).runWith(TestSink())

      SystemMaterializer(system).materializer
        .asInstanceOf[PhasedFusingActorMaterializer]
        .supervisor
        .tell(StreamSupervisor.GetChildren, testActor)
      val ref = expectMsgType[Children].children.find(_.path.toString contains "fileSource").get
      try assertDispatcher(ref, ActorAttributes.IODispatcher.dispatcher)
      finally p.cancel()
    }

    "allow overriding the dispatcher using Attributes" in {
      val p = FileIO
        .fromPath(manyLines)
        .addAttributes(ActorAttributes.dispatcher("akka.actor.default-dispatcher"))
        .runWith(TestSink())

      SystemMaterializer(system).materializer
        .asInstanceOf[PhasedFusingActorMaterializer]
        .supervisor
        .tell(StreamSupervisor.GetChildren, testActor)
      val ref = expectMsgType[Children].children.find(_.path.toString contains "fileSource").get
      try assertDispatcher(ref, "akka.actor.default-dispatcher")
      finally p.cancel()
    }

    "not signal onComplete more than once" in {
      FileIO
        .fromPath(testFile, 2 * TestText.length)
        .runWith(TestSink())
        .requestNext(ByteString(TestText, UTF_8.name))
        .expectComplete()
        .expectNoMessage(1.second)
    }
  }

  override def afterTermination(): Unit = {
    fs.close()
  }

}
