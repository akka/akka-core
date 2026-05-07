/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
class RecipeKeepAlive extends RecipeSpec {

  "Recipe for injecting keepalive messages" must {

    "work" in {
      val keepaliveMessage = ByteString(11)

      //#inject-keepalive
      import scala.concurrent.duration._
      val injectKeepAlive: Flow[ByteString, ByteString, NotUsed] =
        Flow[ByteString].keepAlive(1.second, () => keepaliveMessage)
      //#inject-keepalive

      // No need to test, this is a built-in stage with proper tests
    }
  }

}
