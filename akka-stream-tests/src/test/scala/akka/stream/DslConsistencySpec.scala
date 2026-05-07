/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.lang.reflect.Method
import java.lang.reflect.Modifier

import scala.annotation.nowarn

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object DslConsistencySpec {
  class ScalaSubSource[Out, Mat]
      extends impl.SubFlowImpl[Out, Out, Mat, scaladsl.Source[Out, Mat]#Repr, scaladsl.RunnableGraph[Mat]](
        null,
        null,
        null)
  class ScalaSubFlow[In, Out, Mat]
      extends impl.SubFlowImpl[Out, Out, Mat, scaladsl.Flow[In, Out, Mat]#Repr, scaladsl.Sink[In, Mat]](
        null,
        null,
        null)
}

class DslConsistencySpec extends AnyWordSpec with Matchers {

  val sFlowClass: Class[_] = classOf[akka.stream.scaladsl.Flow[_, _, _]]
  val jFlowClass: Class[_] = classOf[akka.stream.javadsl.Flow[_, _, _]]

  val sSubFlowClass: Class[_] = classOf[DslConsistencySpec.ScalaSubFlow[_, _, _]]
  val jSubFlowClass: Class[_] = classOf[akka.stream.javadsl.SubFlow[_, _, _]]

  val sSourceClass: Class[_] = classOf[akka.stream.scaladsl.Source[_, _]]
  val jSourceClass: Class[_] = classOf[akka.stream.javadsl.Source[_, _]]

  val sSubSourceClass: Class[_] = classOf[DslConsistencySpec.ScalaSubSource[_, _]]
  val jSubSourceClass: Class[_] = classOf[akka.stream.javadsl.SubSource[_, _]]

  val sSinkClass: Class[_] = classOf[akka.stream.scaladsl.Sink[_, _]]
  val jSinkClass: Class[_] = classOf[akka.stream.javadsl.Sink[_, _]]

  val jRunnableGraphClass: Class[_] = classOf[akka.stream.javadsl.RunnableGraph[_]]
  val sRunnableGraphClass: Class[_] = classOf[akka.stream.scaladsl.RunnableGraph[_]]

  val ignore: Set[String] =
    Set("equals", "hashCode", "notify", "notifyAll", "wait", "toString", "getClass") ++
    Set("productArity", "canEqual", "productPrefix", "copy", "productIterator", "productElement") ++
    Set("productElementName", "productElementNames") ++
    Set("_1") ++
    Set(
      "create",
      "apply",
      "ops",
      "appendJava",
      "andThen",
      "andThenMat",
      "isIdentity",
      "withAttributes",
      "transformMaterializing") ++
    Set("asScala", "asJava", "deprecatedAndThen", "deprecatedAndThenMat")

  val graphHelpers = Set(
    "zipGraph",
    "zipWithGraph",
    "zipLatestGraph",
    "zipLatestWithGraph",
    "zipAllFlow",
    "mergeGraph",
    "mergeLatestGraph",
    "mergePreferredGraph",
    "mergePrioritizedGraph",
    "mergeSortedGraph",
    "interleaveGraph",
    "concatGraph",
    "prependGraph",
    "alsoToGraph",
    "wireTapGraph",
    "orElseGraph",
    "divertToGraph")

  val allowMissing: Map[Class[_], Set[String]] = Map(
    jFlowClass -> graphHelpers,
    jSourceClass -> (graphHelpers ++ Set("watch", "ask")),
    // Java subflows can only be nested using .via and .to (due to type system restrictions)
    jSubFlowClass -> (graphHelpers ++ Set("groupBy", "splitAfter", "splitWhen", "subFlow", "watch", "ask")),
    jSubSourceClass -> (graphHelpers ++ Set("groupBy", "splitAfter", "splitWhen", "subFlow", "watch", "ask")),
    sFlowClass -> Set("of"),
    sSourceClass -> Set("adapt", "from", "watch"),
    sSinkClass -> Set("adapt"),
    sSubFlowClass -> Set(),
    sSubSourceClass -> Set(),
    sRunnableGraphClass -> Set("builder"))

  @nowarn
  def materializing(m: Method): Boolean = m.getParameterTypes.contains(classOf[ActorMaterializer])

  def assertHasMethod(c: Class[_], name: String): Unit = {
    // include class name to get better error message
    if (!allowMissing.getOrElse(c, Set.empty).contains(name))
      c.getMethods.collect { case m if !ignore(m.getName) => c.getName + "." + m.getName } should contain(
        c.getName + "." + name)
  }

  "Java and Scala DSLs" must {

    (("Source" -> List[Class[_]](sSourceClass, jSourceClass)) ::
    ("SubSource" -> List[Class[_]](sSubSourceClass, jSubSourceClass)) ::
    ("Flow" -> List[Class[_]](sFlowClass, jFlowClass)) ::
    ("SubFlow" -> List[Class[_]](sSubFlowClass, jSubFlowClass)) ::
    ("Sink" -> List[Class[_]](sSinkClass, jSinkClass)) ::
    ("RunnableFlow" -> List[Class[_]](sRunnableGraphClass, jRunnableGraphClass)) ::
    Nil).foreach {
      case (element, classes) =>
        s"provide same $element transforming operators" in {
          val allOps =
            (for {
              c <- classes
              m <- c.getMethods
              if !Modifier.isStatic(m.getModifiers)
              if !ignore(m.getName)
              if !m.getName.contains("$")
              if !materializing(m)
            } yield m.getName).toSet

          for (c <- classes; op <- allOps)
            assertHasMethod(c, op)
        }

        s"provide same $element materializing operators" in {
          val materializingOps =
            (for {
              c <- classes
              m <- c.getMethods
              if !Modifier.isStatic(m.getModifiers)
              if !ignore(m.getName)
              if !m.getName.contains("$")
              if materializing(m)
            } yield m.getName).toSet

          for (c <- classes; op <- materializingOps)
            assertHasMethod(c, op)
        }

    }
  }

}
