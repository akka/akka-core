/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import io.aeron.Aeron
import io.aeron.driver.MediaDriver
import org.agrona.IoUtil

import akka.actor.ExtendedActorSystem
import akka.remote.artery.aeron.AeronSink.GaveUpMessageException
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.SocketUtil

class AeronSinkSpec extends AkkaSpec("""
    akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {

  val driver = MediaDriver.launchEmbedded()

  val aeron = {
    val ctx = new Aeron.Context
    ctx.aeronDirectoryName(driver.aeronDirectoryName)
    Aeron.connect(ctx)
  }

  val idleCpuLevel = 5
  val taskRunner = {
    val r = new TaskRunner(system.asInstanceOf[ExtendedActorSystem], idleCpuLevel)
    r.start()
    r
  }

  val pool = new EnvelopeBufferPool(1034 * 1024, 128)

  override def afterTermination(): Unit = {
    taskRunner.stop()
    aeron.close()
    driver.close()
    IoUtil.delete(new File(driver.aeronDirectoryName), true)
    super.afterTermination()
  }

  "AeronSink" must {

    "give up sending after given duration" in {
      val port = SocketUtil.temporaryLocalPort(udp = true)
      val channel = s"aeron:udp?endpoint=localhost:$port"

      Source
        .fromGraph(new AeronSource(channel, 1, aeron, taskRunner, pool, 0))
        // fail receiver stream on first message
        .map(_ => throw new RuntimeException("stop") with NoStackTrace)
        .runWith(Sink.ignore)

      // use large enough messages to fill up buffers
      val payload = new Array[Byte](100000)
      val done = Source(1 to 1000)
        .map(_ => payload)
        .map { _ =>
          val envelope = pool.acquire()
          envelope.byteBuffer.put(payload)
          envelope.byteBuffer.flip()
          envelope
        }
        .runWith(new AeronSink(channel, 1, aeron, taskRunner, pool, 500.millis))

      // without the give up timeout the stream would not complete/fail
      intercept[GaveUpMessageException] {
        Await.result(done, 5.seconds)
      }
    }

  }

}
