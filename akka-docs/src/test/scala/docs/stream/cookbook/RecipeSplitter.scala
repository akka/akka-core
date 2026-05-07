/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RecipeSplitter extends AnyWordSpec with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val system: ActorSystem = ActorSystem("Test")

  "Splitter" should {
    " simple split " in {

      //#Simple-Split
      //Sample Source
      val source: Source[String, NotUsed] = Source(List("1-2-3", "2-3", "3-4"))

      val ret = source
        .map(s => s.split("-").toList)
        .mapConcat(identity)
        //Sub-streams logic
        .map(s => s.toInt)
        .runWith(Sink.seq)

      //Verify results

      ret.futureValue should be(Vector(1, 2, 3, 2, 3, 3, 4))
      //#Simple-Split

    }

    " aggregate split" in {

      //#Aggregate-Split
      //Sample Source
      val source: Source[String, NotUsed] = Source(List("1-2-3", "2-3", "3-4"))

      val result = source
        .map(s => s.split("-").toList)
        //split all messages into sub-streams
        .splitWhen(_ => true)
        //now split each collection
        .mapConcat(identity)
        //Sub-streams logic
        .map(s => s.toInt)
        //aggregate each sub-stream
        .reduce((a, b) => a + b)
        //and merge back the result into the original stream
        .mergeSubstreams
        .runWith(Sink.seq);

      //Verify results
      result.futureValue should be(Vector(6, 5, 7))
      //#Aggregate-Split

    }

  }

  override protected def afterAll(): Unit = system.terminate()

}
