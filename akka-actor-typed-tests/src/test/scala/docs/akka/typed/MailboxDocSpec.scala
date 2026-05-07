/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.Behavior
import akka.actor.typed.MailboxSelector
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

class MailboxDocSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.load("mailbox-config-sample.conf"))
    with AnyWordSpecLike
    with LogCapturing {

  "Specifying mailbox through props" must {
    "work" in {
      val probe = createTestProbe[Done]()
      val childBehavior: Behavior[String] = Behaviors.empty
      val parent: Behavior[Unit] = Behaviors.setup { context =>
        // #select-mailbox
        context.spawn(childBehavior, "bounded-mailbox-child", MailboxSelector.bounded(100))

        val props = MailboxSelector.fromConfig("my-app.my-special-mailbox")
        context.spawn(childBehavior, "from-config-mailbox-child", props)
        // #select-mailbox

        probe.ref ! Done
        Behaviors.stopped
      }
      spawn(parent)

      probe.receiveMessage()
    }
  }

}
