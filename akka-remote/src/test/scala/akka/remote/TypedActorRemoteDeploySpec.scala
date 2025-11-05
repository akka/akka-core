/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.remote

import scala.annotation.nowarn
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import TypedActorRemoteDeploySpec._
import com.typesafe.config._

import akka.actor.{ ActorSystem, Deploy, TypedActor, TypedProps }
import akka.testkit.AkkaSpec

object TypedActorRemoteDeploySpec {
  val conf = ConfigFactory.parseString("""
      akka.actor.provider = remote
      akka.remote.artery.canonical.port = 0
      akka.remote.use-unsafe-remote-features-outside-cluster = on
      akka.actor.allow-java-serialization = on
      """)

  trait RemoteNameService {
    def getName: Future[String]
    def getNameSelfDeref: Future[String]
  }

  class RemoteNameServiceImpl extends RemoteNameService {
    @nowarn
    def getName: Future[String] = Future.successful(TypedActor.context.system.name)

    @nowarn
    def getNameSelfDeref: Future[String] = TypedActor.self[RemoteNameService].getName
  }

}

class TypedActorRemoteDeploySpec extends AkkaSpec(conf) {
  val remoteName = "remote-sys"
  val remoteSystem = ActorSystem(remoteName, conf)
  val remoteAddress = RARP(remoteSystem).provider.getDefaultAddress

  @nowarn
  def verify[T](f: RemoteNameService => Future[T], expected: T) = {
    val ts = TypedActor(system)
    val echoService: RemoteNameService =
      ts.typedActorOf(TypedProps[RemoteNameServiceImpl]().withDeploy(Deploy(scope = RemoteScope(remoteAddress))))
    Await.result(f(echoService), 3.seconds) should ===(expected)
    val actor = ts.getActorRefFor(echoService)
    system.stop(actor)
    watch(actor)
    expectTerminated(actor)
  }

  "Typed actors" must {

    "be possible to deploy remotely and communicate with" in {
      verify({ _.getName }, remoteName)
    }

    "be possible to deploy remotely and be able to dereference self" in {
      verify({ _.getNameSelfDeref }, remoteName)
    }

  }

  override def afterTermination(): Unit = {
    shutdown(remoteSystem)
  }

}
