/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success
import scala.util.control.NoStackTrace

import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs

import akka.Done
import akka.stream.AbruptTerminationException
import akka.stream.ActorAttributes
import akka.stream.ActorAttributes._
import akka.stream.ActorMaterializer
import akka.stream.Supervision._
import akka.stream.SystemMaterializer
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.impl.StreamSupervisor
import akka.stream.impl.StreamSupervisor.Children
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.EventFilter
import akka.util.ByteString

class MapWithResourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  private val fs = Jimfs.newFileSystem("MapWithResourceSpec", Configuration.unix())
  private val ex = new Exception("TEST") with NoStackTrace

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
    val file = Files.createFile(fs.getPath("/testMapWithResource.dat"))
    Files.write(file, manyLines.getBytes(StandardCharsets.UTF_8))
  }
  private def newBufferedReader() = Files.newBufferedReader(manyLinesPath, StandardCharsets.UTF_8)

  private def readLines(reader: BufferedReader, maxCount: Int): List[String] = {
    if (maxCount == 0) {
      return List.empty
    }
    @tailrec
    def loop(builder: ListBuffer[String], n: Int): ListBuffer[String] = {
      if (n == 0) {
        builder
      } else {
        val line = reader.readLine()
        if (line eq null)
          builder
        else {
          builder += line
          loop(builder, n - 1)
        }
      }
    }
    loop(ListBuffer.empty, maxCount).result()
  }

  "MapWithResource" must {
    "can read contents from a file" in {
      val p = Source(List(1, 10, 20, 30))
        .mapWithResource(() => newBufferedReader())((reader, count) => {
          readLines(reader, count)
        }, reader => {
          reader.close()
          None
        })
        .mapConcat(identity)
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
        .repeat(1)
        .take(100)
        .mapWithResource(() => newBufferedReader())((reader, _) => {
          val s = reader.readLine()
          if (s != null && s.contains("b")) throw TE("") else s
        }, reader => {
          reader.close()
          None
        })
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
        .repeat(1)
        .take(100)
        .mapWithResource(() => newBufferedReader())((reader, _) => {
          val s = reader.readLine()
          if (s != null && s.contains("b")) throw TE("") else s
        }, reader => {
          reader.close()
          None
        })
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
        .repeat(1)
        .mapWithResource(() => newBufferedReader())((reader, _) => {
          val s = reader.read(buffer)
          if (s > 0) Some(ByteString(buffer.mkString("")).take(s)) else None
        }, reader => {
          reader.close()
          None
        })
        .takeWhile(_.isDefined)
        .collect {
          case Some(bytes) => bytes
        }
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
        .single(1)
        .mapWithResource(() => newBufferedReader())((reader, _) => Option(reader.readLine()), reader => {
          reader.close()
          None
        })
        .runWith(TestSink())

      SystemMaterializer(system).materializer
        .asInstanceOf[PhasedFusingActorMaterializer]
        .supervisor
        .tell(StreamSupervisor.GetChildren, testActor)
      val ref = expectMsgType[Children].children.find(_.path.toString contains "mapWithResource").get
      try assertDispatcher(ref, ActorAttributes.IODispatcher.dispatcher)
      finally p.cancel()
    }

    "fail when create throws exception" in {
      EventFilter[TE](occurrences = 1).intercept {
        val p = Source
          .single(1)
          .mapWithResource[BufferedReader, String](() => throw TE(""))((reader, _) => reader.readLine(), reader => {
            reader.close()
            None
          })
          .runWith(Sink.asPublisher(false))
        val c = TestSubscriber.manualProbe[String]()
        p.subscribe(c)

        c.expectSubscription()
        c.expectError(TE(""))
      }
    }

    "fail when close throws exception" in {
      val (pub, sub) = TestSource[Int]()
        .mapWithResource(() => Iterator("a"))((it, _) => if (it.hasNext) Some(it.next()) else None, _ => throw TE(""))
        .collect { case Some(line) => line }
        .toMat(TestSink())(Keep.both)
        .run()

      pub.ensureSubscription()
      sub.ensureSubscription()
      sub.request(1)
      pub.sendNext(1)
      sub.expectNext("a")
      pub.sendComplete()
      sub.expectError(TE(""))
    }

    "not close the resource twice when read fails" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .repeat(1)
        .mapWithResource(() => 23)( // the best resource there is
          (_, _) => throw TE("failing read"),
          _ => {
            closedCounter.incrementAndGet()
            None
          })
        .runWith(TestSink[Int]())

      probe.request(1)
      probe.expectError(TE("failing read"))
      closedCounter.get() should ===(1)
    }

    "not close the resource twice when read fails and then close fails" in {
      val closedCounter = new AtomicInteger(0)
      val probe = Source
        .repeat(1)
        .mapWithResource(() => 23)((_, _) => throw TE("failing read"), _ => {
          closedCounter.incrementAndGet()
          if (closedCounter.get == 1) throw TE("boom")
          None
        })
        .runWith(TestSink[Int]())

      EventFilter[TE](occurrences = 1).intercept {
        probe.request(1)
        probe.expectError(TE("boom"))
      }
      closedCounter.get() should ===(1)
    }

    "will close the resource when upstream complete" in {
      val closedCounter = new AtomicInteger(0)
      val (pub, sub) = TestSource[Int]()
        .mapWithResource(() => newBufferedReader())((reader, count) => readLines(reader, count), reader => {
          reader.close()
          closedCounter.incrementAndGet()
          Some(List("End"))
        })
        .mapConcat(identity)
        .toMat(TestSink())(Keep.both)
        .run()
      sub.expectSubscription().request(2)
      pub.sendNext(1)
      sub.expectNext(manyLinesArray(0))
      pub.sendComplete()
      sub.expectNext("End")
      sub.expectComplete()
      closedCounter.get shouldBe (1)
    }

    "will close the resource when upstream fail" in {
      val closedCounter = new AtomicInteger(0)
      val (pub, sub) = TestSource[Int]()
        .mapWithResource(() => newBufferedReader())((reader, count) => readLines(reader, count), reader => {
          reader.close()
          closedCounter.incrementAndGet()
          Some(List("End"))
        })
        .mapConcat(identity)
        .toMat(TestSink())(Keep.both)
        .run()
      sub.expectSubscription().request(2)
      pub.sendNext(1)
      sub.expectNext(manyLinesArray(0))
      pub.sendError(ex)
      sub.expectNext("End")
      sub.expectError(ex)
      closedCounter.get shouldBe (1)
    }

    "will close the resource when downstream cancel" in {
      val closedCounter = new AtomicInteger(0)
      val (pub, sub) = TestSource[Int]()
        .mapWithResource(() => newBufferedReader())((reader, count) => readLines(reader, count), reader => {
          reader.close()
          closedCounter.incrementAndGet()
          Some(List("End"))
        })
        .mapConcat(identity)
        .toMat(TestSink())(Keep.both)
        .run()
      val subscription = sub.expectSubscription()
      subscription.request(2)
      pub.sendNext(1)
      sub.expectNext(manyLinesArray(0))
      subscription.cancel()
      pub.expectCancellation()
      closedCounter.get shouldBe (1)
    }

    "will close the resource when downstream fail" in {
      val closedCounter = new AtomicInteger(0)
      val (pub, sub) = TestSource[Int]()
        .mapWithResource(() => newBufferedReader())((reader, count) => readLines(reader, count), reader => {
          reader.close()
          closedCounter.incrementAndGet()
          Some(List("End"))
        })
        .mapConcat(identity)
        .toMat(TestSink())(Keep.both)
        .run()
      sub.request(2)
      pub.sendNext(2)
      sub.expectNext(manyLinesArray(0))
      sub.expectNext(manyLinesArray(1))
      sub.cancel(ex)
      pub.expectCancellationWithCause(ex)
      closedCounter.get shouldBe (1)
    }

    "will close the resource on abrupt materializer termination" in {
      @nowarn("msg=deprecated")
      val mat = ActorMaterializer()
      val promise = Promise[Done]()
      val matVal = Source
        .single(1)
        .mapWithResource(() => {
          newBufferedReader()
        })((reader, count) => readLines(reader, count), reader => {
          reader.close()
          promise.complete(Success(Done))
          Some(List("End"))
        })
        .mapConcat(identity)
        .runWith(Sink.never)(mat)
      mat.shutdown()
      matVal.failed.futureValue shouldBe an[AbruptTerminationException]
      Await.result(promise.future, 3.seconds) shouldBe Done
    }

  }
  override def afterTermination(): Unit = {
    fs.close()
  }
}
