/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.annotation.nowarn
import scala.collection.immutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object ReflectSpec {
  final class A
  final class B

  class One(@nowarn("msg=never used") a: A)
  class Two(@nowarn("msg=never used") a: A, @nowarn("msg=never used") b: B)

  class MultipleOne(a: A, b: B) {
    def this(a: A) = this(a, null)
    def this(b: B) = this(null, b)
  }
}

class ReflectSpec extends AnyWordSpec with Matchers {

  import akka.util.ReflectSpec._

  "Reflect#findConstructor" must {

    "deal with simple 1 matching case" in {
      Reflect.findConstructor(classOf[One], immutable.Seq(new A))
    }
    "deal with 2-params 1 matching case" in {
      Reflect.findConstructor(classOf[Two], immutable.Seq(new A, new B))
    }
    "deal with 2-params where one is `null` 1 matching case" in {
      Reflect.findConstructor(classOf[Two], immutable.Seq(new A, null))
      Reflect.findConstructor(classOf[Two], immutable.Seq(null, new B))
    }
    "deal with `null` in 1 matching case" in {
      val constructor = Reflect.findConstructor(classOf[One], immutable.Seq(null))
      constructor.newInstance(null)
    }
    "deal with multiple constructors" in {
      Reflect.findConstructor(classOf[MultipleOne], immutable.Seq(new A))
      Reflect.findConstructor(classOf[MultipleOne], immutable.Seq(new B))
      Reflect.findConstructor(classOf[MultipleOne], immutable.Seq(new A, new B))
    }
    "throw when multiple matching constructors" in {
      intercept[IllegalArgumentException] {
        Reflect.findConstructor(classOf[MultipleOne], immutable.Seq(null))
      }
    }
  }

}
