/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs

import akka.stream.ActorAttributes
import akka.stream.ActorAttributes._
import akka.stream.Supervision._
import akka.stream.SystemMaterializer
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.impl.StreamSupervisor
import akka.stream.impl.StreamSupervisor.Children
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.EventFilter
import akka.util.ByteString

class UnfoldResourceSourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  private val fs = Jimfs.newFileSystem("UnfoldResourceSourceSpec", Configuration.unix())

  private val manyLines = {
    ("a" * 100 + "\n") * 10 +
    ("b" * 100 + "\n") * 10 +
    ("c" * 100 + "\n") * 10 +
    ("d" * 100 + "\n") * 10 +
    ("e" * 100 + "\n") * 10 +
    ("f" * 100 + "\n") * 10
  }
  private val manyLinesArray = manyLines.split("\n")

  private val manyLinesPath = {
    val file = Files.createFile(fs.getPath("/test.dat"))
    Files.write(file, manyLines.getBytes(StandardCharsets.UTF_8))
  }
  private def newBufferedReader() = Files.newBufferedReader(manyLinesPath, StandardCharsets.UTF_8)

  "Unfold Resource Source" must {
    "read contents from a file" in {
      val p = Source
        .unfoldResource[String, BufferedReader](
          () => newBufferedReader(),
          reader => Option(reader.readLine()),
          reader => reader.close())
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[String]()
      p.subscribe(c)
      val sub = c.expectSubscription()

      val chunks = manyLinesArray.toList.iterator

      sub.request(1)
      c.expectNext() should ===(chunks.next())
      sub.request(1)
      c.expectNext() should ===(chunks.next())
      c.expectNoMessage(300.millis)

      while (chunks.hasNext) {
        sub.request(1)
        c.expectNext() should ===(chunks.next())
      }
      sub.request(1)

      c.expectComplete()
    }

    "continue when Strategy is Resume and exception happened" in {
      val p = Source
        .unfoldResource[String, BufferedReader](() => newBufferedReader(), reader => {
          val s = reader.readLine()
          if (s != null && s.contains("b")) throw TE("") else Option(s)
        }, reader => reader.close())
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[String]()

      p.subscribe(c)
      val sub = c.expectSubscription()

      (0 to 49).foreach(i => {
        sub.request(1)
        c.expectNext() should ===(if (i < 10) manyLinesArray(i) else manyLinesArray(i + 10))
      })
      sub.request(1)
      c.expectComplete()
    }

    "close and open stream again when Strategy is Restart" in {
      val p = Source
        .unfoldResource[String, BufferedReader](() => newBufferedReader(), reader => {
          val s = reader.readLine()
          if (s != null && s.contains("b")) throw TE("") else Option(s)
        }, reader => reader.close())
        .withAttributes(supervisionStrategy(restartingDecider))
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[String]()

      p.subscribe(c)
      val sub = c.expectSubscription()

      (0 to 19).foreach(_ => {
        sub.request(1)
        c.expectNext() should ===(manyLinesArray(0))
      })
      sub.cancel()
    }

    "work with ByteString as well" in {
      val chunkSize = 50
      val buffer = new Array[Char](chunkSize)
      val p = Source
        .unfoldResource[ByteString, Reader](() => newBufferedReader(), reader => {
          val s = reader.read(buffer)
          if (s > 0) Some(ByteString(buffer.mkString("")).take(s)) else None
        }, reader => reader.close())
        .runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[ByteString]()

      var remaining = manyLines
      def nextChunk() = {
        val (chunk, rest) = remaining.splitAt(chunkSize)
        remaining = rest
        chunk
      }

      p.subscribe(c)
      val sub = c.expectSubscription()

      (0 to 121).foreach(_ => {
        sub.request(1)
        c.expectNext().utf8String should ===(nextChunk().toString)
      })
      sub.request(1)
      c.expectComplete()
    }

    "use dedicated blocking-io-dispatcher by default" in {
      val p = Source
        .unfoldResource[String, BufferedReader](
          () => newBufferedReader(),
          reader => Option(reader.readLine()),
          reader => reader.close())
        .runWith(TestSink())

      SystemMaterializer(system).materializer
        .asInstanceOf[PhasedFusingActorMaterializer]
        .supervisor
        .tell(StreamSupervisor.GetChildren, testActor)
      val ref = expectMsgType[Children].children.find(_.path.toString contains "unfoldResourceSource").get
      try assertDispatcher(ref, ActorAttributes.IODispatcher.dispatcher)
      finally p.cancel()
    }

    "fail when create throws exception" in {
      EventFilter[TE](occurrences = 1).intercept {
        val p = Source
          .unfoldResource[String, BufferedReader](
            () => throw TE(""),
            reader => Option(reader.readLine()),
            reader => reader.close())
          .runWith(Sink.asPublisher(false))
        val c = TestSubscriber.manualProbe[String]()
        p.subscribe(c)

        c.expectSubscription()
        c.expectError(TE(""))
      }
    }

    "fail when close throws exception" in {
      val out = TestSubscriber.probe[String]()

      EventFilter[TE](occurrences = 1).intercept {
        Source
          .unfoldResource[String, Iterator[String]](
            () => Iterator("a"),
            it => if (it.hasNext) Some(it.next()) else None,
            _ => throw TE(""))
          .runWith(Sink.fromSubscriber(out))

        out.request(61)
        out.expectNext("a")
        out.expectError(TE(""))
      }
    }

    // issue #24924
    "not close the resource twice when read fails" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .unfoldResource[Int, Int](
          () => 23, // the best resource there is
          _ => throw TE("failing read"),
          _ => closedCounter.incrementAndGet())
        .runWith(TestSink[Int]())

      probe.request(1)
      probe.expectError(TE("failing read"))
      closedCounter.get() should ===(1)
    }

    // issue #24924
    "not close the resource twice when read fails and then close fails" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .unfoldResource[Int, Int](
          () => 23, // the best resource there is
          _ => throw TE("failing read"), { _ =>
            closedCounter.incrementAndGet()
            if (closedCounter.get == 1) throw TE("boom")
          })
        .runWith(TestSink[Int]())

      EventFilter[TE](occurrences = 1).intercept {
        probe.request(1)
        probe.expectError(TE("boom"))
      }
      closedCounter.get() should ===(1)
    }

  }
  override def afterTermination(): Unit = {
    fs.close()
  }
}
