/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import scala.concurrent.Await
import akka.stream.Supervision
import akka.stream.scaladsl._
import akka.testkit.AkkaSpec
import akka.stream.ActorAttributes
import scala.concurrent.duration._

class FlowErrorDocSpec extends AkkaSpec {

  "demonstrate fail stream" in {
    //#stop
    val source = Source(0 to 5).map(100 / _)
    val result = source.runWith(Sink.fold(0)(_ + _))
    // division by zero will fail the stream and the
    // result here will be a Future completed with Failure(ArithmeticException)
    //#stop

    intercept[ArithmeticException] {
      Await.result(result, 3.seconds)
    }
  }

  "demonstrate resume stream" in {
    //#resume
    val decider: Supervision.Decider = {
      case _: ArithmeticException => Supervision.Resume
      case _                      => Supervision.Stop
    }
    val source = Source(0 to 5).map(100 / _)
    val runnableGraph =
      source.toMat(Sink.fold(0)(_ + _))(Keep.right)

    val withCustomSupervision = runnableGraph.withAttributes(ActorAttributes.supervisionStrategy(decider))

    val result = withCustomSupervision.run()
    // the element causing division by zero will be dropped
    // result here will be a Future completed with Success(228)
    //#resume

    Await.result(result, 3.seconds) should be(228)
  }

  "demonstrate resume section" in {
    //#resume-section
    val decider: Supervision.Decider = {
      case _: ArithmeticException => Supervision.Resume
      case _                      => Supervision.Stop
    }
    val flow = Flow[Int]
      .filter(100 / _ < 50)
      .map(elem => 100 / (5 - elem))
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
    val source = Source(0 to 5).via(flow)

    val result = source.runWith(Sink.fold(0)(_ + _))
    // the elements causing division by zero will be dropped
    // result here will be a Future completed with Success(150)
    //#resume-section

    Await.result(result, 3.seconds) should be(150)
  }

  "demonstrate restart section" in {
    //#restart-section
    val decider: Supervision.Decider = {
      case _: IllegalArgumentException => Supervision.Restart
      case _                           => Supervision.Stop
    }
    val flow = Flow[Int]
      .scan(0) { (acc, elem) =>
        if (elem < 0) throw new IllegalArgumentException("negative not allowed")
        else acc + elem
      }
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
    val source = Source(List(1, 3, -1, 5, 7)).via(flow)
    val result = source.limit(1000).runWith(Sink.seq)
    // the negative element cause the scan stage to be restarted,
    // i.e. start from 0 again
    // result here will be a Future completed with Success(Vector(0, 1, 4, 0, 5, 12))
    //#restart-section

    Await.result(result, 3.seconds) should be(Vector(0, 1, 4, 0, 5, 12))
  }

  "demonstrate recover" in {
    //#recover
    Source(0 to 6)
      .map(
        n =>
          // assuming `4` and `5` are unexpected values that could throw exception
          if (List(4, 5).contains(n)) throw new RuntimeException(s"Boom! Bad value found: $n")
          else n.toString)
      .recover {
        case e: RuntimeException => e.getMessage
      }
      .runForeach(println)
    //#recover

    /*
Output:
//#recover-output
0
1
2
3                         // last element before failure
Boom! Bad value found: 4  // first element on failure
//#recover-output
   */
  }

  "demonstrate recoverWithRetries" in {
    //#recoverWithRetries
    val planB = Source(List("five", "six", "seven", "eight"))

    Source(0 to 10)
      .map(n =>
        if (n < 5) n.toString
        else throw new RuntimeException("Boom!"))
      .recoverWithRetries(attempts = 1, {
        case _: RuntimeException => planB
      })
      .runForeach(println)
    //#recoverWithRetries

    /*
Output:
//#recoverWithRetries-output
0
1
2
3
4
five
six
seven
eight
//#recoverWithRetries-output
   */
  }

}
