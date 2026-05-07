/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import akka.actor.{ Actor, Props }
import docs.CompileOnlySpec
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

//#props-edge-cases-value-class
case class MyValueClass(v: Int) extends AnyVal

//#props-edge-cases-value-class
@nowarn("msg=never used") // sample snippets
class PropsEdgeCaseSpec extends AnyWordSpec with CompileOnlySpec {
  "value-class-edge-case-example" in compileOnlySpec {
    //#props-edge-cases-value-class-example
    class ValueActor(value: MyValueClass) extends Actor {
      def receive = {
        case multiplier: Long => sender() ! (value.v * multiplier)
      }
    }
    val valueClassProp = Props(classOf[ValueActor], MyValueClass(5)) // Unsupported
    //#props-edge-cases-value-class-example

    //#props-edge-cases-default-values
    class DefaultValueActor(a: Int, b: Int = 5) extends Actor {
      def receive = {
        case x: Int => sender() ! ((a + x) * b)
      }
    }

    val defaultValueProp1 = Props(classOf[DefaultValueActor], 2.0) // Unsupported

    class DefaultValueActor2(b: Int = 5) extends Actor {
      def receive = {
        case x: Int => sender() ! (x * b)
      }
    }
    val defaultValueProp2 = Props[DefaultValueActor2]() // Unsupported
    val defaultValueProp3 = Props(classOf[DefaultValueActor2]) // Unsupported
    //#props-edge-cases-default-values
  }
}
