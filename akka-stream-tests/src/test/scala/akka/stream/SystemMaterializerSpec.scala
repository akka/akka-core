/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.concurrent.Future

import org.scalatest.concurrent.ScalaFutures

import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.StreamSpec

class SystemMaterializerSpec extends StreamSpec with ScalaFutures {

  def compileOnly(): Unit = {
    Source(1 to 3).to(Sink.ignore).run()
    Source(1 to 3).runWith(Sink.ignore)
    Source(1 to 3).runFold(0)((acc, elem) => acc + elem)
    Source(1 to 3).runFoldAsync(0)((acc, elem) => Future.successful(acc + elem))
    Source(1 to 3).runForeach(_ => ())
    Source(1 to 3).runReduce(_ + _)
  }

  "The SystemMaterializer" must {

    "be implicitly provided when implicit actor system is in scope" in {
      val result = Source(1 to 3).toMat(Sink.seq)(Keep.right).run()
      result.futureValue should ===(Seq(1, 2, 3))
    }
  }

}

class SystemMaterializerEagerStartupSpec extends StreamSpec {

  "The SystemMaterializer" must {

    "be eagerly started on system startup" in {
      system.hasExtension(SystemMaterializer.lookup) should ===(true)
    }
  }

}
