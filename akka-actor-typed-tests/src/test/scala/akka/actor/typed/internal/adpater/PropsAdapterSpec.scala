/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adpater

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor
import akka.actor.typed.ActorTags
import akka.actor.typed.MailboxSelector
import akka.actor.typed.Props
import akka.actor.typed.internal.adapter.PropsAdapter
import akka.actor.typed.scaladsl.Behaviors

class PropsAdapterSpec extends AnyWordSpec with Matchers {

  "PropsAdapter" should {
    "default to akka.dispatch.SingleConsumerOnlyUnboundedMailbox" in {
      val props: Props = Props.empty
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.mailbox should ===("akka.actor.typed.default-mailbox")

      val props2: Props = MailboxSelector.defaultMailbox()
      val pa2: actor.Props = PropsAdapter(() => Behaviors.empty, props2, rethrowTypedFailure = false)
      pa2.mailbox should ===("akka.actor.typed.default-mailbox")
    }
    "adapt dispatcher from config" in {
      val props: Props = Props.empty.withDispatcherFromConfig("some.path")
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.dispatcher should ===("some.path")
    }
    "adapt dispatcher same as parent" in {
      val props: Props = Props.empty.withDispatcherSameAsParent
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.dispatcher should ===("..")
    }
    "adapt mailbox from config" in {
      val props: Props = MailboxSelector.fromConfig("some.path")
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.mailbox should ===("some.path")
    }
    "adapt bounded mailbox" in {
      val props: Props = MailboxSelector.bounded(24)
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.mailbox should ===("bounded-capacity:24")
    }
    "adapt tags" in {
      val props: Props = ActorTags.create("my-tag")
      val pa: actor.Props = PropsAdapter(() => Behaviors.empty, props, rethrowTypedFailure = false)
      pa.deploy.tags should ===(Set("my-tag"))
    }

  }
}
