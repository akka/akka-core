/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.annotation.nowarn

import akka.testkit.AkkaSpec

object PropsCreationSpec {

  final class A

  final class B

  class OneParamActor(@nowarn("msg=never used") blackhole: A) extends Actor {
    override def receive = Actor.emptyBehavior
  }

  class TwoParamActor(@nowarn("msg=never used") blackhole1: A, @nowarn("msg=never used") blackhole2: B) extends Actor {
    override def receive = Actor.emptyBehavior
  }

  object OneParamActorCreator extends akka.japi.Creator[Actor] {
    override def create(): Actor = new OneParamActor(null)
  }

}

class PropsCreationSpec extends AkkaSpec("""
  # test is using Java serialization and relies on serialize-creators=on
  akka.actor.serialize-creators = on
  akka.actor.no-serialization-verification-needed-class-prefix = []
  akka.actor.allow-java-serialization = on
  akka.actor.warn-about-java-serializer-usage = off
  """) {

  import akka.actor.PropsCreationSpec._

  "Props" must {
    "work with creator" in {
      val p = Props(new OneParamActor(null))
      system.actorOf(p)
    }
    "work with classOf" in {
      val p = Props(classOf[OneParamActor], null)
      system.actorOf(p)
    }
    "work with classOf + 1 `null` param" in {
      val p = Props(classOf[OneParamActor], null)
      system.actorOf(p)
    }
    "work with classOf + 2 `null` params" in {
      val p = Props(classOf[TwoParamActor], null, null)
      system.actorOf(p)
    }
  }

  "Props Java API" must {
    "work with create(creator)" in {
      @nowarn
      val p = Props.create(OneParamActorCreator)
      system.actorOf(p)
    }
    "work with create(class, creator)" in {
      val p = Props.create(classOf[Actor], OneParamActorCreator)
      system.actorOf(p)
    }
  }

}
