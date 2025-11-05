/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.event.Logging
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter
import akka.testkit.TestEvent

trait ActorSystemLifecycle {

  protected var _system: ActorSystem = _

  implicit final def system: ActorSystem = _system

  def additionalConfig: Config = ConfigFactory.empty()

  def shutdownTimeout: FiniteDuration = 10.seconds

  @BeforeClass
  def createActorSystem(): Unit = {
    _system = ActorSystem(Logging.simpleName(getClass), additionalConfig.withFallback(AkkaSpec.testConf))
    _system.eventStream.publish(TestEvent.Mute(EventFilter[RuntimeException]("Test exception")))
  }

  @AfterClass
  def shutdownActorSystem(): Unit = {
    try {
      Await.ready(system.terminate(), shutdownTimeout)
    } catch {
      case _: TimeoutException =>
        val msg = "Failed to stop [%s] within [%s] \n%s".format(
          system.name,
          shutdownTimeout,
          system.asInstanceOf[ActorSystemImpl].printTree)
        throw new RuntimeException(msg)
    }
  }

}
