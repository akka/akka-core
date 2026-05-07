/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.{ actor => classic }
import akka.actor.{ ExtendedActorSystem, IgnoreActorRef }
import akka.actor.typed.{ ActorRef, ActorRefResolver, ActorSystem }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._

class ActorRefIgnoreSerializationSpec extends AnyWordSpec with ScalaFutures with Matchers with BeforeAndAfterAll {

  private var system1: ActorSystem[String] = _
  private var system2: ActorSystem[String] = _

  val config = ConfigFactory.parseString("""
      akka {
        loglevel = debug
        actor.provider = cluster
        remote.artery {
          canonical {
            hostname = 127.0.0.1
            port = 0
          }
        }
      }
      """)

  override protected def beforeAll(): Unit = {
    system1 = ActorSystem(Behaviors.empty[String], "sys1", config)
    system2 = ActorSystem(Behaviors.empty[String], "sys2", config)
  }

  override protected def afterAll(): Unit = {
    system1.terminate()
    system2.terminate()
  }

  "ActorSystem.ignoreRef (in typed)" should {

    "return a serializable ActorRef that can be sent between two ActorSystems using remote" in {

      val ignoreRef = system1.ignoreRef[String]
      val remoteRefStr = ActorRefResolver(system1).toSerializationFormat(ignoreRef)

      withClue("check ActorRef path stays untouched, ie: /local/ignore") {
        remoteRefStr shouldBe IgnoreActorRef.path.toString
      }

      withClue("check ActorRef path stays untouched when deserialized by another actor system") {
        val deserRef: ActorRef[String] = ActorRefResolver(system2).resolveActorRef[String](remoteRefStr)
        deserRef.path shouldBe IgnoreActorRef.path
        (deserRef should be).theSameInstanceAs(system2.ignoreRef[String])
      }
    }

    "return same instance when deserializing it twice (IgnoreActorRef is cached)" in {

      val resolver = ActorRefResolver(system1)

      withClue("using the same type") {
        val deserRef1 = resolver.resolveActorRef[String](resolver.toSerializationFormat(system1.ignoreRef[String]))
        val deserRef2 = resolver.resolveActorRef[String](resolver.toSerializationFormat(system1.ignoreRef[String]))
        (deserRef1 should be).theSameInstanceAs(deserRef2)
      }

      withClue("using different types") {
        val deserRef1 = resolver.resolveActorRef[String](resolver.toSerializationFormat(system1.ignoreRef[String]))
        val deserRef2 = resolver.resolveActorRef[Int](resolver.toSerializationFormat(system1.ignoreRef[Int]))
        (deserRef1 should be).theSameInstanceAs(deserRef2)
      }

    }
  }

  "IgnoreActorRef (in classic)" should {

    "return a serializable ActorRef that can be sent between two ActorSystems using remote (akka classic)" in {

      val ignoreRef = system1.ignoreRef[String].toClassic
      val remoteRefStr = ignoreRef.path.toSerializationFormatWithAddress(system1.address)

      withClue("check ActorRef path stays untouched, ie: /local/ignore") {
        remoteRefStr shouldBe IgnoreActorRef.path.toString
      }

      withClue("check ActorRef path stays untouched when deserialized by another actor system") {
        val providerSys2 = system2.classicSystem.asInstanceOf[ExtendedActorSystem].provider
        val deserRef: classic.ActorRef = providerSys2.resolveActorRef(remoteRefStr)
        deserRef.path shouldBe IgnoreActorRef.path
        (deserRef should be).theSameInstanceAs(system2.ignoreRef[String].toClassic)
      }
    }

    "return same instance when deserializing it twice (IgnoreActorRef is cached)" in {

      val ignoreRef = system1.ignoreRef[String].toClassic
      val remoteRefStr = ignoreRef.path.toSerializationFormat
      val providerSys1 = system1.classicSystem.asInstanceOf[ExtendedActorSystem].provider

      val deserRef1 = providerSys1.resolveActorRef(remoteRefStr)
      val deserRef2 = providerSys1.resolveActorRef(remoteRefStr)
      (deserRef1 should be).theSameInstanceAs(deserRef2)
    }

  }
}
