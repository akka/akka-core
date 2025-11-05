/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

abstract class TestSuperclass {
  def name: String
}

class TestClassWithStringConstructor(val name: String) extends TestSuperclass
class TestClassWithDefaultConstructor extends TestSuperclass {
  override def name = "default"
}

class DynamicAccessSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  val system = ActorSystem()

  "The DynamicAccess of a system" should {
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

    "instantiate an object with a default constructor" in {
      val instance: Try[TestClassWithDefaultConstructor] = dynamicAccess
        .createInstanceFor[TestClassWithDefaultConstructor]("akka.actor.TestClassWithDefaultConstructor", Nil)
      instance match {
        case Success(i) => i shouldNot be(null)
        case Failure(t) => fail(t)
      }
    }

    "throw a ClassNotFound exception when the class is not found" in {
      dynamicAccess.createInstanceFor[TestClassWithDefaultConstructor]("foo.NonExistingClass", Nil) match {
        case Success(instance) =>
          fail(s"Expected failure, found $instance")
        case Failure(e) =>
          e shouldBe a[ClassNotFoundException]
      }
    }

    "try different constructors with recoverWith" in {
      instantiateWithDefaultOrStringCtor("akka.actor.TestClassWithStringConstructor").get.name shouldBe "string ctor argument"
      instantiateWithDefaultOrStringCtor("akka.actor.TestClassWithDefaultConstructor").get.name shouldBe "default"
      instantiateWithDefaultOrStringCtor("akka.actor.foo.NonExistingClass") match {
        case Failure(t) =>
          t shouldBe a[ClassNotFoundException]
        case Success(instance) =>
          fail(s"unexpected instance $instance")
      }
    }

    "know if a class exists on the classpath or not" in {
      dynamicAccess.classIsOnClasspath("i.just.made.it.up.to.hurt.Myself") should ===(false)
      dynamicAccess.classIsOnClasspath("akka.actor.Actor") should ===(true)
    }

    def instantiateWithDefaultOrStringCtor(fqcn: String): Try[TestSuperclass] =
      // recoverWith doesn't work with scala 2.13.0-M5
      // https://github.com/scala/bug/issues/11242
      dynamicAccess.createInstanceFor[TestSuperclass](fqcn, Nil) match {
        case s: Success[TestSuperclass] => s
        case Failure(_: NoSuchMethodException) =>
          dynamicAccess
            .createInstanceFor[TestSuperclass](fqcn, immutable.Seq((classOf[String], "string ctor argument")))
        case f: Failure[_] => f
      }

  }

  override def afterAll() = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }
}
