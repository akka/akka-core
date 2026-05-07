/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor.typed

import akka.actor.typed._
import akka.actor.typed.scaladsl._
import com.typesafe.config.ConfigFactory

import scala.annotation.nowarn
import scala.concurrent.{ ExecutionContext, Future }

// #blocking-in-future
object BlockingFutureActor {
  def apply(): Behavior[Int] =
    Behaviors.setup { context =>
      implicit val executionContext: ExecutionContext = context.executionContext

      Behaviors.receiveMessage { i =>
        triggerFutureBlockingOperation(i)
        Behaviors.same
      }
    }

  def triggerFutureBlockingOperation(i: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    println(s"Calling blocking Future: $i")
    Future {
      Thread.sleep(5000) //block for 5 seconds
      println(s"Blocking future finished $i")
    }
  }
}
// #blocking-in-future

// #separate-dispatcher
object SeparateDispatcherFutureActor {
  def apply(): Behavior[Int] =
    Behaviors.setup { context =>
      implicit val executionContext: ExecutionContext =
        context.system.dispatchers.lookup(DispatcherSelector.fromConfig("my-blocking-dispatcher"))

      Behaviors.receiveMessage { i =>
        triggerFutureBlockingOperation(i)
        Behaviors.same
      }
    }

  def triggerFutureBlockingOperation(i: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    println(s"Calling blocking Future: $i")
    Future {
      Thread.sleep(5000) //block for 5 seconds
      println(s"Blocking future finished $i")
    }
  }
}
// #separate-dispatcher

@nowarn("msg=never used") // sample snippets
object BlockingDispatcherSample {
  def main(args: Array[String]): Unit = {
    // #blocking-main
    val root = Behaviors.setup[Nothing] { context =>
      for (i <- 1 to 100) {
        context.spawn(BlockingFutureActor(), s"futureActor-$i") ! i
        context.spawn(PrintActor(), s"printActor-$i") ! i
      }
      Behaviors.empty
    }
    val system = ActorSystem[Nothing](root, "BlockingDispatcherSample")
    // #blocking-main
  }
}

@nowarn("msg=never used") // sample snippets
object SeparateDispatcherSample {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseString("""
      //#my-blocking-dispatcher-config
      my-blocking-dispatcher {
        type = Dispatcher
        executor = "thread-pool-executor"
        thread-pool-executor {
          fixed-pool-size = 16
        }
        throughput = 1
      }
      //#my-blocking-dispatcher-config
      """)

    // #separate-dispatcher-main
    val root = Behaviors.setup[Nothing] { context =>
      for (i <- 1 to 100) {
        context.spawn(SeparateDispatcherFutureActor(), s"futureActor-$i") ! i
        context.spawn(PrintActor(), s"printActor-$i") ! i
      }
      Behaviors.ignore
    }
    // #separate-dispatcher-main
    val system = ActorSystem[Nothing](root, "SeparateDispatcherSample", config)

  }
}
