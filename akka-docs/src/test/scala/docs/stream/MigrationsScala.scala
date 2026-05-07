/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.stream.scaladsl._
import akka.testkit.AkkaSpec

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
class MigrationsScala extends AkkaSpec {

  "Examples in migration guide" must {
    "compile" in {
      lazy val dontExecuteMe = {
        //#expand-continually
        Flow[Int].expand(Iterator.continually(_))
        //#expand-continually
        //#expand-state
        Flow[Int].expand(i => {
          var state = 0
          Iterator.continually({
            state += 1
            (i, state)
          })
        })
        //#expand-state

        //#async
        val flow = Flow[Int].map(_ + 1)
        Source(1 to 10).via(flow.async)
        //#async
      }
    }
  }

}
