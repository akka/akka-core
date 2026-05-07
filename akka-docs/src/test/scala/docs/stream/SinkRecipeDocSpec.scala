/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.stream.scaladsl.{ Sink, Source }
import docs.stream.cookbook.RecipeSpec

import scala.concurrent.Future

class SinkRecipeDocSpec extends RecipeSpec {
  "Sink.foreachAsync" must {
    "processing each element asynchronously" in {
      def asyncProcessing(value: Int): Future[Unit] = Future { println(value) }(system.dispatcher)
      //#forseachAsync-processing
      //def asyncProcessing(value: Int): Future[Unit] = _

      Source(1 to 100).runWith(Sink.foreachAsync(10)(asyncProcessing))
      //#forseachAsync-processing
    }
  }
}
