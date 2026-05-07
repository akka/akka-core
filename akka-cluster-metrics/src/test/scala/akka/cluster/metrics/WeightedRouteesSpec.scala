/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import com.typesafe.config.ConfigFactory

import akka.actor.Address
import akka.actor.RootActorPath
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import akka.testkit.AkkaSpec

class WeightedRouteesSpec extends AkkaSpec(ConfigFactory.parseString("""
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0
      """)) {

  val protocol = "akka"

  val a1 = Address(protocol, "sys", "a1", 2551)
  val b1 = Address(protocol, "sys", "b1", 2551)
  val c1 = Address(protocol, "sys", "c1", 2551)
  val d1 = Address(protocol, "sys", "d1", 2551)

  val routeeA = ActorSelectionRoutee(system.actorSelection(RootActorPath(a1) / "user" / "a"))
  val routeeB = ActorSelectionRoutee(system.actorSelection(RootActorPath(b1) / "user" / "b"))
  val routeeC = ActorSelectionRoutee(system.actorSelection(RootActorPath(c1) / "user" / "c"))
  val routees = Vector(routeeA, routeeB, routeeC)
  val testActorRoutee = ActorRefRoutee(testActor)

  "WeightedRoutees" must {

    "allocate weighted routees" in {
      val weights = Map(a1 -> 1, b1 -> 3, c1 -> 10)
      val weighted = new WeightedRoutees(routees, a1, weights)

      weighted(1) should ===(routeeA)
      (2 to 4).foreach { weighted(_) should ===(routeeB) }
      (5 to 14).foreach { weighted(_) should ===(routeeC) }
      weighted.total should ===(14)
    }

    "check boundaries" in {
      val empty = new WeightedRoutees(Vector(), a1, Map.empty)
      empty.isEmpty should ===(true)
      intercept[IllegalArgumentException] {
        empty.total
      }

      val empty2 = new WeightedRoutees(Vector(routeeA), a1, Map(a1 -> 0))
      empty2.isEmpty should ===(true)
      intercept[IllegalArgumentException] {
        empty2.total
      }
      intercept[IllegalArgumentException] {
        empty2(0)
      }

      val weighted = new WeightedRoutees(routees, a1, Map.empty)
      weighted.total should ===(3)
      intercept[IllegalArgumentException] {
        weighted(0)
      }
      intercept[IllegalArgumentException] {
        weighted(4)
      }
    }

    "allocate routees for undefined weight" in {
      val weights = Map(a1 -> 1, b1 -> 7)
      val weighted = new WeightedRoutees(routees, a1, weights)

      weighted(1) should ===(routeeA)
      (2 to 8).foreach { weighted(_) should ===(routeeB) }
      // undefined, uses the mean of the weights, i.e. 4
      (9 to 12).foreach { weighted(_) should ===(routeeC) }
      weighted.total should ===(12)
    }

    "allocate weighted local routees" in {
      val weights = Map(a1 -> 2, b1 -> 1, c1 -> 10)
      val routees2 = Vector(testActorRoutee, routeeB, routeeC)
      val weighted = new WeightedRoutees(routees2, a1, weights)

      (1 to 2).foreach { weighted(_) should ===(testActorRoutee) }
      (3 to weighted.total).foreach { weighted(_) should not be (testActorRoutee) }
    }

    "not allocate ref with weight zero" in {
      val weights = Map(a1 -> 0, b1 -> 2, c1 -> 10)
      val weighted = new WeightedRoutees(routees, a1, weights)

      (1 to weighted.total).foreach { weighted(_) should not be (routeeA) }
    }

  }
}
