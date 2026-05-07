/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.future

import language.postfixOps
import akka.testkit._
import akka.actor.{ Actor, ActorRef, Props, Status }
import akka.util.Timeout

import scala.concurrent.duration._
import java.lang.IllegalStateException
import akka.actor.typed.ActorSystem

import scala.annotation.nowarn
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success }

object FutureDocSpec {

  class MyActor extends Actor {
    def receive = {
      case x: String       => sender() ! x.toUpperCase
      case x: Int if x < 0 => sender() ! Status.Failure(new ArithmeticException("Negative values not supported"))
      case x: Int          => sender() ! x
    }
  }

  case object GetNext

  class OddActor extends Actor {
    var n = 1
    def receive = {
      case GetNext =>
        sender() ! n
        n += 2
    }
  }

  //#pipe-to-usage
  class ActorUsingPipeTo(target: ActorRef) extends Actor {
    // akka.pattern.pipe needs to be imported
    import akka.pattern.{ ask, pipe }
    // implicit ExecutionContext should be in scope
    implicit val ec: ExecutionContext = context.dispatcher
    implicit val timeout: Timeout = 5.seconds

    def receive = {
      case _ =>
        val future = target ? "some message"
        future.pipeTo(sender()) // use the pipe pattern
    }
  }
  //#pipe-to-usage

  //#pipe-to-returned-data
  case class UserData(data: String)
  case class UserActivity(activity: String)
  //#pipe-to-returned-data

  //#pipe-to-user-data-actor
  class UserDataActor extends Actor {
    import UserDataActor._

    //holds the user data internally
    var internalData: UserData = UserData("initial data")

    def receive = {
      case Get =>
        sender() ! internalData
    }
  }

  object UserDataActor {
    case object Get
  }
  //#pipe-to-user-data-actor

  //#pipe-to-user-activity-actor
  trait UserActivityRepository {
    def queryHistoricalActivities(userId: String): Future[List[UserActivity]]
  }

  class UserActivityActor(val userId: String, repository: UserActivityRepository) extends Actor {
    import akka.pattern.pipe
    import UserActivityActor._
    implicit val ec: ExecutionContext = context.dispatcher

    def receive = {
      case Get =>
        // user's historical activities are retrieved
        // via the separate repository
        repository.queryHistoricalActivities(userId).pipeTo(sender())
    }
  }

  object UserActivityActor {
    case object Get
  }
  //#pipe-to-user-activity-actor

  //#pipe-to-proxy-actor
  class UserProxyActor(userData: ActorRef, userActivities: ActorRef) extends Actor {
    import UserProxyActor._
    import akka.pattern.{ ask, pipe }
    implicit val ec: ExecutionContext = context.dispatcher

    implicit val timeout: Timeout = Timeout(5 seconds)

    def receive = {
      case GetUserData =>
        (userData ? UserDataActor.Get).pipeTo(sender())
      case GetUserActivities =>
        (userActivities ? UserActivityActor.Get).pipeTo(sender())
    }
  }
  //#pipe-to-proxy-actor

  //#pipe-to-proxy-messages
  object UserProxyActor {
    sealed trait Message
    case object GetUserData extends Message
    case object GetUserActivities extends Message
  }
  //#pipe-to-proxy-messages

}

@nowarn("msg=never used") // sample snippets
class FutureDocSpec extends AkkaSpec {
  import FutureDocSpec._
  import system.dispatcher

  val println: PartialFunction[Any, Unit] = { case _ => }

  "demonstrate usage custom ExecutionContext" in {
    val yourExecutorServiceGoesHere = java.util.concurrent.Executors.newSingleThreadExecutor()
    //#diy-execution-context
    import scala.concurrent.{ ExecutionContext, Promise }

    implicit val ec = ExecutionContext.fromExecutorService(yourExecutorServiceGoesHere)

    // Do stuff with your brand new shiny ExecutionContext
    val f = Promise.successful("foo")

    // Then shut your ExecutionContext down at some
    // appropriate place in your program/application
    ec.shutdown()
    //#diy-execution-context
  }

  "demonstrate usage of blocking from actor" in {
    val actor = system.actorOf(Props[MyActor]())
    val msg = "hello"
    //#ask-blocking
    import scala.concurrent.Await
    import akka.pattern.ask
    import akka.util.Timeout
    import scala.concurrent.duration._

    implicit val timeout: Timeout = 5.seconds
    val future = actor ? msg // enabled by the “ask” import
    val result = Await.result(future, timeout.duration).asInstanceOf[String]
    //#ask-blocking

    result should be("HELLO")
  }

  "demonstrate usage of mapTo" in {
    val actor = system.actorOf(Props[MyActor]())
    val msg = "hello"
    implicit val timeout: Timeout = 5.seconds
    //#map-to
    import scala.concurrent.Future
    import akka.pattern.ask

    val future: Future[String] = ask(actor, msg).mapTo[String]
    //#map-to
    Await.result(future, timeout.duration) should be("HELLO")
  }

  "demonstrate usage of simple future eval" in {
    //#future-eval
    import scala.concurrent.Await
    import scala.concurrent.Future
    import scala.concurrent.duration._

    val future = Future {
      "Hello" + "World"
    }
    future.foreach(println)
    //#future-eval
    Await.result(future, 3 seconds) should be("HelloWorld")
  }

  "demonstrate usage of map" in {
    //#map
    val f1 = Future {
      "Hello" + "World"
    }
    val f2 = f1.map { x =>
      x.length
    }
    f2.foreach(println)
    //#map
    val result = Await.result(f2, 3 seconds)
    result should be(10)
    f1.value should be(Some(Success("HelloWorld")))
  }

  "demonstrate wrong usage of nested map" in {
    //#wrong-nested-map
    val f1 = Future {
      "Hello" + "World"
    }
    val f2 = Future.successful(3)
    val f3 = f1.map { x =>
      f2.map { y =>
        x.length * y
      }
    }
    f3.foreach(println)
    //#wrong-nested-map
    Await.ready(f3, 3 seconds)
  }

  "demonstrate usage of flatMap" in {
    //#flat-map
    val f1 = Future {
      "Hello" + "World"
    }
    val f2 = Future.successful(3)
    val f3 = f1.flatMap { x =>
      f2.map { y =>
        x.length * y
      }
    }
    f3.foreach(println)
    //#flat-map
    val result = Await.result(f3, 3 seconds)
    result should be(30)
  }

  "demonstrate usage of filter" in {
    //#filter
    val future1 = Future.successful(4)
    val future2 = future1.filter(_ % 2 == 0)

    future2.foreach(println)

    val failedFilter = future1.filter(_ % 2 == 1).recover {
      // When filter fails, it will have a java.util.NoSuchElementException
      case m: NoSuchElementException => 0
    }

    failedFilter.foreach(println)
    //#filter
    val result = Await.result(future2, 3 seconds)
    result should be(4)
    val result2 = Await.result(failedFilter, 3 seconds)
    result2 should be(0) //Can only be 0 when there was a MatchError
  }

  "demonstrate usage of for comprehension" in {
    //#for-comprehension
    val f = for {
      a <- Future(10 / 2) // 10 / 2 = 5
      b <- Future(a + 1) //  5 + 1 = 6
      c <- Future(a - 1) //  5 - 1 = 4
      if c > 3 // Future.filter
    } yield b * c //  6 * 4 = 24

    // Note that the execution of futures a, b, and c
    // are not done in parallel.

    f.foreach(println)
    //#for-comprehension
    val result = Await.result(f, 3 seconds)
    result should be(24)
  }

  "demonstrate wrong way of composing" in {
    val actor1 = system.actorOf(Props[MyActor]())
    val actor2 = system.actorOf(Props[MyActor]())
    val actor3 = system.actorOf(Props[MyActor]())
    val msg1 = 1
    val msg2 = 2
    implicit val timeout: Timeout = 5.seconds
    import scala.concurrent.Await
    import akka.pattern.ask
    //#composing-wrong

    val f1 = ask(actor1, msg1)
    val f2 = ask(actor2, msg2)

    val a = Await.result(f1, 3 seconds).asInstanceOf[Int]
    val b = Await.result(f2, 3 seconds).asInstanceOf[Int]

    val f3 = ask(actor3, (a + b))

    val result = Await.result(f3, 3 seconds).asInstanceOf[Int]
    //#composing-wrong
    result should be(3)
  }

  "demonstrate composing" in {
    val actor1 = system.actorOf(Props[MyActor]())
    val actor2 = system.actorOf(Props[MyActor]())
    val actor3 = system.actorOf(Props[MyActor]())
    val msg1 = 1
    val msg2 = 2
    implicit val timeout: Timeout = 5.seconds
    import scala.concurrent.Await
    import akka.pattern.ask
    //#composing

    val f1 = ask(actor1, msg1)
    val f2 = ask(actor2, msg2)

    val f3 = for {
      a <- f1.mapTo[Int]
      b <- f2.mapTo[Int]
      c <- ask(actor3, (a + b)).mapTo[Int]
    } yield c

    f3.foreach(println)
    //#composing
    val result = Await.result(f3, 3 seconds).asInstanceOf[Int]
    result should be(3)
  }

  "demonstrate usage of sequence with actors" in {
    implicit val timeout: Timeout = 5.seconds
    val oddActor = system.actorOf(Props[OddActor]())
    //#sequence-ask
    // oddActor returns odd numbers sequentially from 1 as a List[Future[Int]]
    val listOfFutures = List.fill(100)(akka.pattern.ask(oddActor, GetNext).mapTo[Int])

    // now we have a Future[List[Int]]
    val futureList = Future.sequence(listOfFutures)

    // Find the sum of the odd numbers
    val oddSum = futureList.map(_.sum)
    oddSum.foreach(println)
    //#sequence-ask
    Await.result(oddSum, 3 seconds).asInstanceOf[Int] should be(10000)
  }

  "demonstrate usage of sequence" in {
    //#sequence
    val futureList = Future.sequence((1 to 100).toList.map(x => Future(x * 2 - 1)))
    val oddSum = futureList.map(_.sum)
    oddSum.foreach(println)
    //#sequence
    Await.result(oddSum, 3 seconds).asInstanceOf[Int] should be(10000)
  }

  "demonstrate usage of traverse" in {
    //#traverse
    val futureList = Future.traverse((1 to 100).toList)(x => Future(x * 2 - 1))
    val oddSum = futureList.map(_.sum)
    oddSum.foreach(println)
    //#traverse
    Await.result(oddSum, 3 seconds).asInstanceOf[Int] should be(10000)
  }

  "demonstrate usage of fold" in {
    //#fold
    // Create a sequence of Futures
    val futures = for (i <- 1 to 1000) yield Future(i * 2)
    val futureSum = Future.foldLeft(futures)(0)(_ + _)
    futureSum.foreach(println)
    //#fold
    Await.result(futureSum, 3 seconds) should be(1001000)
  }

  "demonstrate usage of reduce" in {
    //#reduce
    // Create a sequence of Futures
    val futures = for (i <- 1 to 1000) yield Future(i * 2)
    val futureSum = Future.reduceLeft(futures)(_ + _)
    futureSum.foreach(println)
    //#reduce
    Await.result(futureSum, 3 seconds) should be(1001000)
  }

  "demonstrate usage of recover" in {
    implicit val timeout: Timeout = 5.seconds
    val actor = system.actorOf(Props[MyActor]())
    val msg1 = -1
    //#recover
    val future = akka.pattern.ask(actor, msg1).recover {
      case _: ArithmeticException => 0
    }
    future.foreach(println)
    //#recover
    Await.result(future, 3.seconds) should be(0)
  }

  "demonstrate usage of recoverWith" in {
    implicit val timeout: Timeout = 5.seconds
    val actor = system.actorOf(Props[MyActor]())
    val msg1 = -1
    //#try-recover
    val future = akka.pattern.ask(actor, msg1).recoverWith {
      case _: ArithmeticException => Future.successful(0)
      case _: IllegalArgumentException =>
        Future.failed[Int](new IllegalStateException("All br0ken!"))
    }
    future.foreach(println)
    //#try-recover
    Await.result(future, 3 seconds) should be(0)
  }

  "demonstrate usage of zip" in {
    val future1 = Future { "foo" }
    val future2 = Future { "bar" }
    //#zip
    val future3 = future1.zip(future2).map { case (a, b) => a + " " + b }
    future3.foreach(println)
    //#zip
    Await.result(future3, 3 seconds) should be("foo bar")
  }

  "demonstrate usage of andThen" in {
    def loadPage(s: String) = s
    val url = "foo bar"
    def log(cause: Throwable) = ()
    def watchSomeTV(): Unit = ()
    //#and-then
    val result = Future { loadPage(url) }
      .andThen {
        case Failure(exception) => log(exception)
      }
      .andThen {
        case _ => watchSomeTV()
      }
    result.foreach(println)
    //#and-then
    Await.result(result, 3 seconds) should be("foo bar")
  }

  "demonstrate usage of fallbackTo" in {
    val future1 = Future { "foo" }
    val future2 = Future { "bar" }
    val future3 = Future { "pigdog" }
    //#fallback-to
    val future4 = future1.fallbackTo(future2).fallbackTo(future3)
    future4.foreach(println)
    //#fallback-to
    Await.result(future4, 3 seconds) should be("foo")
  }

  "demonstrate usage of onComplete" in {
    val future = Future { "foo" }
    def doSomethingOnSuccess(r: String) = ()
    def doSomethingOnFailure(t: Throwable) = ()
    //#onComplete
    future.onComplete {
      case Success(result)  => doSomethingOnSuccess(result)
      case Failure(failure) => doSomethingOnFailure(failure)
    }
    //#onComplete
    Await.result(future, 3 seconds) should be("foo")
  }

  "demonstrate usage of Future.successful & Future.failed & Future.promise" in {
    //#successful
    val future = Future.successful("Yay!")
    //#successful
    //#failed
    val otherFuture = Future.failed[String](new IllegalArgumentException("Bang!"))
    //#failed
    //#promise
    val promise = Promise[String]()
    val theFuture = promise.future
    promise.success("hello")
    //#promise
    Await.result(future, 3 seconds) should be("Yay!")
    intercept[IllegalArgumentException] { Await.result(otherFuture, 3 seconds) }
    Await.result(theFuture, 3 seconds) should be("hello")
  }

  "demonstrate usage of pattern.after" in {
    import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
    implicit val system: ActorSystem[Nothing] = this.system.toTyped
    //#after
    val delayed =
      akka.pattern.after(200.millis)(Future.failed(new IllegalStateException("OHNOES")))

    val future = Future { Thread.sleep(1000); "foo" }
    val result = Future.firstCompletedOf(Seq(future, delayed))
    //#after
    intercept[IllegalStateException] { Await.result(result, 2 second) }
  }

  "demonstrate pattern.retry" in {
    import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
    val system: ActorSystem[Nothing] = this.system.toTyped
    //#retry
    import akka.actor.typed.scaladsl.adapter._
    implicit val scheduler: akka.actor.Scheduler = system.scheduler.toClassic
    implicit val ec: ExecutionContext = system.executionContext

    //Given some future that will succeed eventually
    @volatile var failCount = 0
    def futureToAttempt() = {
      if (failCount < 5) {
        failCount += 1
        Future.failed(new IllegalStateException(failCount.toString))
      } else Future.successful(5)
    }

    //Return a new future that will retry up to 10 times
    val retried: Future[Int] = akka.pattern.retry(() => futureToAttempt(), attempts = 10, 100 milliseconds)
    //#retry

    Await.result(retried, 1 second) should ===(5)
  }

  "demonstrate context.dispatcher" in {
    //#context-dispatcher
    class A extends Actor {
      import context.dispatcher
      val f = Future("hello")
      def receive = {
        //#receive-omitted
        case _ =>
        //#receive-omitted
      }
    }
    //#context-dispatcher
  }

}
