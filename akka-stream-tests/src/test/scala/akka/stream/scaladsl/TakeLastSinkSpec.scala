/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.{ Await, Future }

import akka.stream.{ AbruptTerminationException, ActorMaterializer, ActorMaterializerSettings }
import akka.stream.testkit.{ StreamSpec, TestPublisher }

@nowarn
class TakeLastSinkSpec extends StreamSpec {

  val settings = ActorMaterializerSettings(system).withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val mat: ActorMaterializer = ActorMaterializer(settings)

  "Sink.takeLast" must {
    "return the last 3 elements" in {
      val input = 1 to 6
      val future: Future[immutable.Seq[Int]] = Source(input).runWith(Sink.takeLast(3))
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(Seq(4, 5, 6))
    }

    "return top three student based on GPA correctly" in {
      implicit val ex = system.dispatcher
      //#takeLast-operator-example
      case class Student(name: String, gpa: Double)

      val students = List(
        Student("Alison", 4.7),
        Student("Adrian", 3.1),
        Student("Alexis", 4),
        Student("Benita", 2.1),
        Student("Kendra", 4.2),
        Student("Jerrie", 4.3)).sortBy(_.gpa)

      val sourceOfStudents = Source(students)

      val result: Future[Seq[Student]] = sourceOfStudents.runWith(Sink.takeLast(3))

      result.foreach { topThree =>
        println("#### Top students ####")
        topThree.reverse.foreach { s =>
          println(s"Name: ${s.name}, GPA: ${s.gpa}")
        }
      }
      /*
          #### Top students ####
          Name: Alison, GPA: 4.7
          Name: Jerrie, GPA: 4.3
          Name: Kendra, GPA: 4.2
       */

      //#takeLast-operator-example

      result.futureValue shouldEqual students.takeRight(3)
    }

    "return the number of elements taken when the stream completes" in {
      val input = 1 to 4
      val future: Future[immutable.Seq[Int]] = Source(input).runWith(Sink.takeLast(5))
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(Seq(1, 2, 3, 4))
    }

    "fail future when stream abruptly terminated" in {
      val mat = ActorMaterializer()
      val probe = TestPublisher.probe()
      val future: Future[immutable.Seq[Int]] =
        Source.fromPublisher(probe).runWith(Sink.takeLast(2))(mat)
      mat.shutdown()
      future.failed.futureValue shouldBe an[AbruptTerminationException]
    }

    "yield empty seq for empty stream" in {
      val future: Future[immutable.Seq[Int]] = Source.empty[Int].runWith(Sink.takeLast(3))
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(Seq.empty)
    }

    "return the last element" in {
      val input = 1 to 4
      val future: Future[immutable.Seq[Int]] = Source(input).runWith(Sink.takeLast(1))
      val result: immutable.Seq[Int] = Await.result(future, remainingOrDefault)
      result should be(Seq(4))
    }
  }
}
