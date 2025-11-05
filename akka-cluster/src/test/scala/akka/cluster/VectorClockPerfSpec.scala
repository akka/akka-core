/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable.{ SortedSet, TreeMap }

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object VectorClockPerfSpec {
  import VectorClock._

  def createVectorClockOfSize(size: Int): (VectorClock, SortedSet[Node]) =
    (1 to size).foldLeft((VectorClock(), SortedSet.empty[Node])) {
      case ((vc, nodes), i) =>
        val node = Node(i.toString)
        (vc :+ node, nodes + node)
    }

  def copyVectorClock(vc: VectorClock): VectorClock = {
    val versions = vc.versions.foldLeft(TreeMap.empty[Node, Long]) {
      case (versions, (n, t)) => versions.updated(Node.fromHash(n), t)
    }
    vc.copy(versions = versions)
  }

}

class VectorClockPerfSpec extends AnyWordSpec with Matchers {
  import VectorClock._
  import VectorClockPerfSpec._

  val clockSize = sys.props.get("akka.cluster.VectorClockPerfSpec.clockSize").getOrElse("1000").toInt
  // increase for serious measurements
  val iterations = sys.props.get("akka.cluster.VectorClockPerfSpec.iterations").getOrElse("1000").toInt

  val (vcBefore, nodes) = createVectorClockOfSize(clockSize)
  val firstNode = nodes.head
  val lastNode = nodes.last
  val middleNode = nodes.drop(clockSize / 2).head
  val vcBaseLast = vcBefore :+ lastNode
  val vcAfterLast = vcBaseLast :+ firstNode
  val vcConcurrentLast = vcBaseLast :+ lastNode
  val vcBaseMiddle = vcBefore :+ middleNode
  val vcAfterMiddle = vcBaseMiddle :+ firstNode
  val vcConcurrentMiddle = vcBaseMiddle :+ middleNode

  def checkThunkFor(vc1: VectorClock, vc2: VectorClock, thunk: (VectorClock, VectorClock) => Unit, times: Int): Unit = {
    val vcc1 = copyVectorClock(vc1)
    val vcc2 = copyVectorClock(vc2)
    for (_ <- 1 to times) {
      thunk(vcc1, vcc2)
    }
  }

  def compareTo(order: Ordering)(vc1: VectorClock, vc2: VectorClock): Unit = {
    vc1.compareTo(vc2) should ===(order)
  }

  def notEqual(vc1: VectorClock, vc2: VectorClock): Unit = {
    vc1 == vc2 should ===(false)
  }

  s"VectorClock comparisons of size $clockSize" must {

    s"do a warm up run $iterations times" in {
      checkThunkFor(vcBaseLast, vcBaseLast, compareTo(Same), iterations)
    }

    s"compare Same a $iterations times" in {
      checkThunkFor(vcBaseLast, vcBaseLast, compareTo(Same), iterations)
    }

    s"compare Before (last) $iterations times" in {
      checkThunkFor(vcBefore, vcBaseLast, compareTo(Before), iterations)
    }

    s"compare After (last) $iterations times" in {
      checkThunkFor(vcAfterLast, vcBaseLast, compareTo(After), iterations)
    }

    s"compare Concurrent (last) $iterations times" in {
      checkThunkFor(vcAfterLast, vcConcurrentLast, compareTo(Concurrent), iterations)
    }

    s"compare Before (middle) $iterations times" in {
      checkThunkFor(vcBefore, vcBaseMiddle, compareTo(Before), iterations)
    }

    s"compare After (middle) $iterations times" in {
      checkThunkFor(vcAfterMiddle, vcBaseMiddle, compareTo(After), iterations)
    }

    s"compare Concurrent (middle) $iterations times" in {
      checkThunkFor(vcAfterMiddle, vcConcurrentMiddle, compareTo(Concurrent), iterations)
    }

    s"compare !== Before (middle) $iterations times" in {
      checkThunkFor(vcBefore, vcBaseMiddle, notEqual, iterations)
    }

    s"compare !== After (middle) $iterations times" in {
      checkThunkFor(vcAfterMiddle, vcBaseMiddle, notEqual, iterations)
    }

    s"compare !== Concurrent (middle) $iterations times" in {
      checkThunkFor(vcAfterMiddle, vcConcurrentMiddle, notEqual, iterations)
    }

  }
}
