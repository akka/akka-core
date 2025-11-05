/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.stream.scaladsl

import akka.testkit.AkkaSpec
import scala.annotation.nowarn

@nowarn // for keeping imports
class FlowPrependSpec extends AkkaSpec {

//#prepend

//#prepend

  "An Prepend flow" should {

    "work in entrance example" in {
      //#prepend
      val ladies = Source(List("Emma", "Emily"))
      val gentlemen = Source(List("Liam", "William"))

      gentlemen.prepend(ladies).runWith(Sink.foreach(println))
      // this will print "Emma", "Emily", "Liam", "William"
      //#prepend
    }

    "work in lazy entrance example" in {
      //#prependLazy
      val ladies = Source(List("Emma", "Emily"))
      val gentlemen = Source(List("Liam", "William"))

      gentlemen.prependLazy(ladies).runWith(Sink.foreach(println))
      // this will print "Emma", "Emily", "Liam", "William"
      //#prependLazy
    }
  }
}
