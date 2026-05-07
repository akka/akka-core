/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.stream.testkit._

class LastSinkSpec extends StreamSpec with ScriptedTest {

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  "A Flow with Sink.last" must {

    "yield the last value" in {
      //#last-operator-example
      val source = Source(1 to 10)
      val result: Future[Int] = source.runWith(Sink.last)
      result.map(println)
      // 10
      //#last-operator-example
      result.futureValue shouldEqual 10
    }

    "yield the first error" in {
      val ex = new RuntimeException("ex")
      (intercept[RuntimeException] {
        Await.result(Source.failed[Int](ex).runWith(Sink.last), 1.second)
      } should be).theSameInstanceAs(ex)
    }

    "yield NoSuchElementException for empty stream" in {
      intercept[NoSuchElementException] {
        Await.result(Source.empty[Int].runWith(Sink.last), 1.second)
      }.getMessage should be("last of empty stream")
    }

  }
  "A Flow with Sink.lastOption" must {

    "yield the last value" in {
      Await.result(Source(1 to 42).map(identity).runWith(Sink.lastOption), 1.second) should be(Some(42))
    }

    "yield the first error" in {
      val ex = new RuntimeException("ex")
      (intercept[RuntimeException] {
        Await.result(Source.failed[Int](ex).runWith(Sink.lastOption), 1.second)
      } should be).theSameInstanceAs(ex)
    }

    "yield None for empty stream" in {
      //#lastOption-operator-example
      val source = Source.empty[Int]
      val result: Future[Option[Int]] = source.runWith(Sink.lastOption)
      result.map(println)
      // None
      //#lastOption-operator-example
      result.futureValue shouldEqual None
    }

  }

}
