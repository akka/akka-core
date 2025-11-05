/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config

import akka.actor.dispatch.ActorModelSpec
import akka.dispatch.DispatcherPrerequisites
import akka.dispatch.MessageDispatcher
import akka.dispatch.MessageDispatcherConfigurator

object CallingThreadDispatcherModelSpec {
  import ActorModelSpec._

  val config = {
    """
      boss {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
    """ +
    // use unique dispatcher id for each test, since MessageDispatcherInterceptor holds state
    (for (n <- 1 to 30)
      yield """
        test-calling-thread-%s {
          type = "akka.testkit.CallingThreadDispatcherModelSpec$CallingThreadDispatcherInterceptorConfigurator"
        }""".format(n)).mkString
  }

  class CallingThreadDispatcherInterceptorConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends MessageDispatcherConfigurator(config, prerequisites) {

    private val instance: MessageDispatcher =
      new CallingThreadDispatcher(this) with MessageDispatcherInterceptor {
        override def id: String = config.getString("id")
      }

    override def dispatcher(): MessageDispatcher = instance

  }

}

class CallingThreadDispatcherModelSpec extends ActorModelSpec(CallingThreadDispatcherModelSpec.config) {
  import ActorModelSpec._

  val dispatcherCount = new AtomicInteger()

  override def interceptedDispatcher(): MessageDispatcherInterceptor = {
    // use new id for each test, since the MessageDispatcherInterceptor holds state
    system.dispatchers
      .lookup("test-calling-thread-" + dispatcherCount.incrementAndGet())
      .asInstanceOf[MessageDispatcherInterceptor]
  }
  override def dispatcherType = "Calling Thread Dispatcher"

}
