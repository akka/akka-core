/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.io

import java.nio.file.{ Files, Paths }
import akka.stream._
import akka.stream.scaladsl.{ FileIO, Sink, Source }
import akka.stream.testkit.Utils._
import akka.util.ByteString
import akka.testkit.AkkaSpec

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

@nowarn("msg=never used") // sample snippets
@nowarn("msg=Unused import")
class StreamFileDocSpec extends AkkaSpec(UnboundedMailboxConfig) {

  implicit val ec: ExecutionContext = system.dispatcher

  // silence sysout
  def println(s: String) = ()

  val file = Files.createTempFile(getClass.getName, ".tmp")

  override def afterTermination() = Files.delete(file)

  {
    //#file-source
    import akka.stream.scaladsl._
    //#file-source
    Thread.sleep(0) // needs a statement here for valid syntax and to avoid "unused" warnings
  }

  {
    //#file-source
    val file = Paths.get("example.csv")
    //#file-source
  }

  {
    //#file-sink
    val file = Paths.get("greeting.txt")
    //#file-sink
  }

  "read data from a file" in {
    //#file-source
    def handle(b: ByteString): Unit //#file-source
    = ()

    //#file-source

    val foreach: Future[IOResult] = FileIO.fromPath(file).to(Sink.ignore).run()
    //#file-source
  }

  "configure dispatcher in code" in {
    //#custom-dispatcher-code
    FileIO.fromPath(file).withAttributes(ActorAttributes.dispatcher("custom-blocking-io-dispatcher"))
    //#custom-dispatcher-code
  }

  "write data into a file" in {
    //#file-sink
    val text = Source.single("Hello Akka Stream!")
    val result: Future[IOResult] = text.map(t => ByteString(t)).runWith(FileIO.toPath(file))
    //#file-sink
  }
}
