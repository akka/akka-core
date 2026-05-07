/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.annotation.nowarn

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.remote.DaemonMsgCreate
import akka.remote.RemoteScope
import akka.remote.serialization.DaemonMsgCreateSerializerAllowJavaSerializationSpec.ActorWithDummyParameter
import akka.remote.serialization.DaemonMsgCreateSerializerAllowJavaSerializationSpec.MyActorWithParam
import akka.routing.FromConfig
import akka.routing.RoundRobinPool
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.testkit.JavaSerializable

object DaemonMsgCreateSerializerAllowJavaSerializationSpec {

  trait EmptyActor extends Actor {
    def receive = Actor.emptyBehavior
  }
  class MyActor extends EmptyActor
  class MyActorWithParam(@nowarn("msg=never used") ignore: String) extends EmptyActor
  class MyActorWithFunParam(@nowarn("msg=never used") fun: Function1[Int, Int]) extends EmptyActor
  class ActorWithDummyParameter(
      @nowarn("msg=never used") javaSerialized: DummyParameter,
      @nowarn("msg=never used") protoSerialized: ActorRef)
      extends EmptyActor
}

case class DummyParameter(val inner: String) extends JavaSerializable

private[akka] trait SerializationVerification { self: AkkaSpec =>

  def ser: Serialization

  def verifySerialization(msg: DaemonMsgCreate): Unit = {
    assertDaemonMsgCreate(
      msg,
      ser.deserialize(ser.serialize(msg).get, classOf[DaemonMsgCreate]).get.asInstanceOf[DaemonMsgCreate])
  }

  def assertDaemonMsgCreate(expected: DaemonMsgCreate, got: DaemonMsgCreate): Unit = {
    // can't compare props.creator when function
    got.props.clazz should ===(expected.props.clazz)
    got.props.args.length should ===(expected.props.args.length)
    got.props.args.zip(expected.props.args).foreach {
      case (g, e) =>
        if (e.isInstanceOf[Function0[_]]) ()
        else if (e.isInstanceOf[Function1[_, _]]) ()
        else g should ===(e)
    }
    got.props.deploy should ===(expected.props.deploy)
    got.deploy should ===(expected.deploy)
    got.path should ===(expected.path)
    got.supervisor should ===(expected.supervisor)
  }
}

class DaemonMsgCreateSerializerAllowJavaSerializationSpec
    extends AkkaSpec("""
  # test is verifying Java serialization  
  akka.actor.allow-java-serialization = on
  akka.actor.warn-about-java-serializer-usage = off
  """)
    with SerializationVerification {

  import DaemonMsgCreateSerializerAllowJavaSerializationSpec._
  val ser = SerializationExtension(system)
  val supervisor = system.actorOf(Props[MyActor](), "supervisor")

  "Serialization" must {

    "resolve DaemonMsgCreateSerializer" in {
      ser.serializerFor(classOf[DaemonMsgCreate]).getClass should ===(classOf[DaemonMsgCreateSerializer])
    }

    "serialize and de-serialize DaemonMsgCreate with function creator" in {
      verifySerialization {
        DaemonMsgCreate(props = Props(new MyActor), deploy = Deploy(), path = "foo", supervisor = supervisor)
      }
    }

    "serialize and de-serialize DaemonMsgCreate with FromClassCreator, with function parameters for Props" in {
      verifySerialization {
        DaemonMsgCreate(
          props = Props(classOf[MyActorWithFunParam], (i: Int) => i + 1),
          deploy = Deploy(),
          path = "foo",
          supervisor = supervisor)
      }
    }

    "serialize and de-serialize DaemonMsgCreate with Deploy and RouterConfig" in {
      verifySerialization {
        // Duration.Inf doesn't equal Duration.Inf, so we use another for test
        // we don't serialize the supervisor strategy, but always fallback to default
        val supervisorStrategy = SupervisorStrategy.defaultStrategy
        val deploy1 = Deploy(
          path = "path1",
          config = ConfigFactory.parseString("a=1"),
          routerConfig = RoundRobinPool(nrOfInstances = 5, supervisorStrategy = supervisorStrategy),
          scope = RemoteScope(Address("akka", "Test", "host1", 1921)),
          dispatcher = "mydispatcher")
        val deploy2 = Deploy(
          path = "path2",
          config = ConfigFactory.parseString("a=2"),
          routerConfig = FromConfig,
          scope = RemoteScope(Address("akka", "Test", "host2", 1922)),
          dispatcher = Deploy.NoDispatcherGiven)
        DaemonMsgCreate(
          props = Props[MyActor]().withDispatcher("my-disp").withDeploy(deploy1),
          deploy = deploy2,
          path = "foo",
          supervisor = supervisor)
      }
    }

  }
}

class DaemonMsgCreateSerializerNoJavaSerializationSpec extends AkkaSpec("""
   akka.actor.allow-java-serialization=off
  """) with SerializationVerification {

  import DaemonMsgCreateSerializerAllowJavaSerializationSpec.MyActor

  val supervisor = system.actorOf(Props[MyActor](), "supervisor")
  val ser = SerializationExtension(system)

  "serialize and de-serialize DaemonMsgCreate with FromClassCreator" in {
    verifySerialization {
      DaemonMsgCreate(props = Props[MyActor](), deploy = Deploy(), path = "foo", supervisor = supervisor)
    }
  }

  "serialize and de-serialize DaemonMsgCreate with FromClassCreator, with null parameters for Props" in {
    verifySerialization {
      DaemonMsgCreate(
        props = Props(classOf[MyActorWithParam], null),
        deploy = Deploy(),
        path = "foo",
        supervisor = supervisor)
    }
  }

  "serialize and de-serialize DaemonMsgCreate with Deploy and RouterConfig" in {
    verifySerialization {
      val deploy1 = Deploy(
        path = "path1",
        config = ConfigFactory.parseString("a=1"),
        // a whole can of worms: routerConfig = RoundRobinPool(nrOfInstances = 5, supervisorStrategy = supervisorStrategy),
        scope = RemoteScope(Address("akka", "Test", "host1", 1921)),
        dispatcher = "mydispatcher")
      val deploy2 = Deploy(
        path = "path2",
        config = ConfigFactory.parseString("a=2"),
        routerConfig = FromConfig,
        scope = RemoteScope(Address("akka", "Test", "host2", 1922)),
        dispatcher = Deploy.NoDispatcherGiven)
      DaemonMsgCreate(
        props = Props[MyActor]().withDispatcher("my-disp").withDeploy(deploy1),
        deploy = deploy2,
        path = "foo",
        supervisor = supervisor)
    }
  }

  "allows for mixing serializers with and without manifests for props parameters" in {
    verifySerialization {
      DaemonMsgCreate(
        // parameters should trigger JavaSerializer for the first one and additional protobuf for the second (?)
        props = Props(classOf[ActorWithDummyParameter], new DummyParameter("dummy"), system.deadLetters),
        deploy = Deploy(),
        path = "foo",
        supervisor = supervisor)
    }
  }

}
