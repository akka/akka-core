/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeFlattenSeq extends RecipeSpec {

  "Recipe for flattening a stream of seqs" must {

    "work" in {

      val someDataSource = Source(List(List("1"), List("2"), List("3", "4", "5"), List("6", "7")))

      //#flattening-seqs
      val myData: Source[List[Message], NotUsed] = someDataSource
      val flattened: Source[Message, NotUsed] = myData.mapConcat(identity)
      //#flattening-seqs

      Await.result(flattened.limit(8).runWith(Sink.seq), 3.seconds) should be(List("1", "2", "3", "4", "5", "6", "7"))

    }

  }

}
