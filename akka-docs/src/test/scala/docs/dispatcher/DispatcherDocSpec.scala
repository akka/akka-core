/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.dispatcher

import akka.testkit.AkkaSpec
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.actor._

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
object DispatcherDocSpec {
  val javaConfig = """
     //#prio-dispatcher-config-java
     prio-dispatcher {
       mailbox-type = "jdocs.dispatcher.DispatcherDocTest$MyPrioMailbox"
       //Other dispatcher configuration goes here
     }
     //#prio-dispatcher-config-java

    //#prio-mailbox-config-java
    prio-mailbox {
      mailbox-type = "docs.dispatcher.DispatcherDocSpec$MyPrioMailbox"
      //Other mailbox configuration goes here
    }
    //#prio-mailbox-config-java

    //#custom-mailbox-config-java
    custom-dispatcher {
      mailbox-requirement =
      "jdocs.dispatcher.MyUnboundedMessageQueueSemantics"
    }

    akka.actor.mailbox.requirements {
      "jdocs.dispatcher.MyUnboundedMessageQueueSemantics" =
      custom-dispatcher-mailbox
    }

    custom-dispatcher-mailbox {
      mailbox-type = "jdocs.dispatcher.MyUnboundedMailbox"
    }
    //#custom-mailbox-config-java
  """

  val config = """
    //#my-dispatcher-config
    my-dispatcher {
      # Dispatcher is the name of the event-based dispatcher
      type = Dispatcher
      # What kind of ExecutionService to use
      executor = "fork-join-executor"
      # Configuration for the fork join pool
      fork-join-executor {
        # Min number of threads to cap factor-based parallelism number to
        parallelism-min = 2
        # Parallelism (threads) ... ceil(available processors * factor)
        parallelism-factor = 2.0
        # Max number of threads to cap factor-based parallelism number to
        parallelism-max = 10
        # Max number of additional threads to spawn by ManagedBlocker
        maximum-spare-threads = 16
      }
      # Throughput defines the maximum number of messages to be
      # processed per actor before the thread jumps to the next actor.
      # Set to 1 for as fair as possible.
      throughput = 100
    }
    //#my-dispatcher-config

    //#my-thread-pool-dispatcher-config
    my-thread-pool-dispatcher {
      # Dispatcher is the name of the event-based dispatcher
      type = Dispatcher
      # What kind of ExecutionService to use
      executor = "thread-pool-executor"
      # Configuration for the thread pool
      thread-pool-executor {
        # minimum number of threads to cap factor-based core number to
        core-pool-size-min = 2
        # No of core threads ... ceil(available processors * factor)
        core-pool-size-factor = 2.0
        # maximum number of threads to cap factor-based number to
        core-pool-size-max = 10
      }
      # Throughput defines the maximum number of messages to be
      # processed per actor before the thread jumps to the next actor.
      # Set to 1 for as fair as possible.
      throughput = 100
    }
    //#my-thread-pool-dispatcher-config
    
    //#my-dispatcher-with-timeouts-config
    my-dispatcher-with-timeouts {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 16
        # Keep alive time for threads
        keep-alive-time = 60s
        # Allow core threads to time out
        allow-core-timeout = off
      }
      # How long time the dispatcher will wait for new actors until it shuts down
      shutdown-timeout = 60s
    }
    //#my-dispatcher-with-timeouts-config

    //#affinity-pool-dispatcher-config
    affinity-pool-dispatcher {
      # Dispatcher is the name of the event-based dispatcher
      type = Dispatcher
      # What kind of ExecutionService to use
      executor = "affinity-pool-executor"
      # Configuration for the thread pool
      affinity-pool-executor {
        # Min number of threads to cap factor-based parallelism number to
        parallelism-min = 8
        # Parallelism (threads) ... ceil(available processors * factor)
        parallelism-factor = 1
        # Max number of threads to cap factor-based parallelism number to
        parallelism-max = 16
      }
      # Throughput defines the maximum number of messages to be
      # processed per actor before the thread jumps to the next actor.
      # Set to 1 for as fair as possible.
      throughput = 100
    }
    //#affinity-pool-dispatcher-config    

    //#fixed-pool-size-dispatcher-config
    blocking-io-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 32
      }
      throughput = 1
    }
    //#fixed-pool-size-dispatcher-config

    //#my-pinned-dispatcher-config
    my-pinned-dispatcher {
      executor = "thread-pool-executor"
      type = PinnedDispatcher
    }
    //#my-pinned-dispatcher-config

    //#my-bounded-config
    my-dispatcher-bounded-queue {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        core-pool-size-factor = 8.0
        max-pool-size-factor  = 16.0
      }
      # Specifies the bounded capacity of the message queue
      mailbox-capacity = 100
      throughput = 3
    }
    //#my-bounded-config

    //#prio-dispatcher-config
    prio-dispatcher {
      mailbox-type = "docs.dispatcher.DispatcherDocSpec$MyPrioMailbox"
      //Other dispatcher configuration goes here
    }
    //#prio-dispatcher-config

    //#dispatcher-deployment-config
    akka.actor.deployment {
      /myactor {
        dispatcher = my-dispatcher
      }
    }
    //#dispatcher-deployment-config

    //#prio-mailbox-config
    prio-mailbox {
      mailbox-type = "docs.dispatcher.DispatcherDocSpec$MyPrioMailbox"
      //Other mailbox configuration goes here
    }
    //#prio-mailbox-config

    //#mailbox-deployment-config

    akka.actor.deployment {
      /priomailboxactor {
        mailbox = prio-mailbox
      }
    }
    //#mailbox-deployment-config

    //#bounded-mailbox-config
    bounded-mailbox {
      mailbox-type = "akka.dispatch.NonBlockingBoundedMailbox"
      mailbox-capacity = 1000 
    }
    //#bounded-mailbox-config

    //#required-mailbox-config

    akka.actor.mailbox.requirements {
      "akka.dispatch.BoundedMessageQueueSemantics" = bounded-mailbox
    }
    //#required-mailbox-config

    //#custom-mailbox-config
    custom-dispatcher {
      mailbox-requirement =
      "docs.dispatcher.MyUnboundedMessageQueueSemantics"
    }

    akka.actor.mailbox.requirements {
      "docs.dispatcher.MyUnboundedMessageQueueSemantics" =
      custom-dispatcher-mailbox
    }

    custom-dispatcher-mailbox {
      mailbox-type = "docs.dispatcher.MyUnboundedMailbox"
    }
    //#custom-mailbox-config

    //#control-aware-mailbox-config
    control-aware-dispatcher {
      mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
      //Other dispatcher configuration goes here
    }
    //#control-aware-mailbox-config

  """

  //#prio-mailbox
  import akka.dispatch.PriorityGenerator
  import akka.dispatch.UnboundedStablePriorityMailbox
  import com.typesafe.config.Config

  // We inherit, in this case, from UnboundedStablePriorityMailbox
  // and seed it with the priority generator
  class MyPrioMailbox(settings: ActorSystem.Settings, config: Config)
      extends UnboundedStablePriorityMailbox(
        // Create a new PriorityGenerator, lower prio means more important
        PriorityGenerator {
          // highpriority messages should be treated first if possible
          case "highpriority" => 0

          // lowpriority messages should be treated last if possible
          case "lowpriority" => 2

          // PoisonPill when no other left
          case PoisonPill => 3

          // We default to 1, which is in between high and low
          case otherwise => 1
        })
  //#prio-mailbox

  //#control-aware-mailbox-messages
  import akka.dispatch.ControlMessage

  case object MyControlMessage extends ControlMessage
  //#control-aware-mailbox-messages

  class MyActor extends Actor {
    def receive = {
      case x =>
    }
  }

  //#required-mailbox-class
  import akka.dispatch.RequiresMessageQueue
  import akka.dispatch.BoundedMessageQueueSemantics

  class MyBoundedActor extends MyActor with RequiresMessageQueue[BoundedMessageQueueSemantics]
  //#required-mailbox-class

  //#require-mailbox-on-actor
  class MySpecialActor extends Actor with RequiresMessageQueue[MyUnboundedMessageQueueSemantics] {
    //#require-mailbox-on-actor
    def receive = {
      case _ =>
    }
    //#require-mailbox-on-actor
    // ...
  }
  //#require-mailbox-on-actor
}

@nowarn("msg=never used") // sample snippets
class DispatcherDocSpec extends AkkaSpec(DispatcherDocSpec.config) {

  import DispatcherDocSpec._

  "defining dispatcher in config" in {
    val context = system
    //#defining-dispatcher-in-config
    import akka.actor.Props
    val myActor = context.actorOf(Props[MyActor](), "myactor")
    //#defining-dispatcher-in-config
  }

  "defining dispatcher in code" in {
    val context = system
    //#defining-dispatcher-in-code
    import akka.actor.Props
    val myActor =
      context.actorOf(Props[MyActor]().withDispatcher("my-dispatcher"), "myactor1")
    //#defining-dispatcher-in-code
  }

  "defining dispatcher with bounded queue" in {
    val dispatcher = system.dispatchers.lookup("my-dispatcher-bounded-queue")
  }

  "defining fixed-pool-size dispatcher" in {
    val context = system
    //#defining-fixed-pool-size-dispatcher
    val myActor =
      context.actorOf(Props[MyActor]().withDispatcher("blocking-io-dispatcher"), "myactor2")
    //#defining-fixed-pool-size-dispatcher
  }

  "defining pinned dispatcher" in {
    val context = system
    //#defining-pinned-dispatcher
    val myActor =
      context.actorOf(Props[MyActor]().withDispatcher("my-pinned-dispatcher"), "myactor3")
    //#defining-pinned-dispatcher
  }

  "defining affinity-pool dispatcher" in {
    val context = system
    //#defining-affinity-pool-dispatcher
    val myActor =
      context.actorOf(Props[MyActor]().withDispatcher("affinity-pool-dispatcher"), "myactor4")
    //#defining-affinity-pool-dispatcher
  }

  "looking up a dispatcher" in {
    //#lookup
    // for use with Futures, Scheduler, etc.
    implicit val executionContext = system.dispatchers.lookup("my-dispatcher")
    //#lookup
  }

  "defining mailbox in config" in {
    val context = system
    //#defining-mailbox-in-config
    import akka.actor.Props
    val myActor = context.actorOf(Props[MyActor](), "priomailboxactor")
    //#defining-mailbox-in-config
  }

  "defining mailbox in code" in {
    val context = system
    //#defining-mailbox-in-code
    import akka.actor.Props
    val myActor = context.actorOf(Props[MyActor]().withMailbox("prio-mailbox"))
    //#defining-mailbox-in-code
  }

  "using a required mailbox" in {
    val context = system
    val myActor = context.actorOf(Props[MyBoundedActor]())
  }

  "defining priority dispatcher" in {
    new AnyRef {
      //#prio-dispatcher

      // We create a new Actor that just prints out what it processes
      class Logger extends Actor {
        val log: LoggingAdapter = Logging(context.system, this)

        self ! "lowpriority"
        self ! "lowpriority"
        self ! "highpriority"
        self ! "pigdog"
        self ! "pigdog2"
        self ! "pigdog3"
        self ! "highpriority"

        self ! PoisonPill

        def receive = {
          case x => log.info(x.toString)
        }
      }
      val a = system.actorOf(Props(classOf[Logger], this).withDispatcher("prio-dispatcher"))

      /*
       * Logs:
       * highpriority
       * highpriority
       * pigdog
       * pigdog2
       * pigdog3
       * lowpriority
       * lowpriority
       */
      //#prio-dispatcher

      watch(a)
      expectMsgPF() { case Terminated(`a`) => () }
    }
  }

  "defining control aware dispatcher" in {
    new AnyRef {
      //#control-aware-dispatcher

      // We create a new Actor that just prints out what it processes
      class Logger extends Actor {
        val log: LoggingAdapter = Logging(context.system, this)

        self ! "foo"
        self ! "bar"
        self ! MyControlMessage
        self ! PoisonPill

        def receive = {
          case x => log.info(x.toString)
        }
      }
      val a = system.actorOf(Props(classOf[Logger], this).withDispatcher("control-aware-dispatcher"))

      /*
       * Logs:
       * MyControlMessage
       * foo
       * bar
       */
      //#control-aware-dispatcher

      watch(a)
      expectMsgPF() { case Terminated(`a`) => () }
    }
  }

  "require custom mailbox on dispatcher" in {
    val myActor = system.actorOf(Props[MyActor]().withDispatcher("custom-dispatcher"))
  }

  "require custom mailbox on actor" in {
    val myActor = system.actorOf(Props[MySpecialActor]())
  }

}
