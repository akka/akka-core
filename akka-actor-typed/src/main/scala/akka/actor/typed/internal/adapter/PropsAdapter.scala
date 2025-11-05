/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adapter

import akka.actor.Deploy
import akka.actor.LocalScope
import akka.actor.TypedCreatorFunctionConsumer
import akka.actor.typed.ActorTags
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.MailboxSelector
import akka.actor.typed.Props
import akka.actor.typed.internal.PropsImpl._
import akka.annotation.InternalApi
import akka.dispatch.Mailboxes

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PropsAdapter {

  private final val TypedCreatorFunctionConsumerClazz = classOf[TypedCreatorFunctionConsumer]
  private final val ActorAdapterClazz = classOf[ActorAdapter[_]]
  private final val DefaultTypedDeploy = Deploy.local.copy(mailbox = "akka.actor.typed.default-mailbox")

  def apply[T](behavior: () => Behavior[T], props: Props, rethrowTypedFailure: Boolean): akka.actor.Props = {
    val deploy =
      if (props eq Props.empty) DefaultTypedDeploy // optimized case with no props specified
      else {
        val deployWithMailbox = props.firstOrElse[MailboxSelector](MailboxSelector.default()) match {
          case _: DefaultMailboxSelector           => DefaultTypedDeploy
          case BoundedMailboxSelector(capacity, _) =>
            // specific support in classic Mailboxes
            DefaultTypedDeploy.copy(mailbox = s"${Mailboxes.BoundedCapacityPrefix}$capacity")
          case MailboxFromConfigSelector(path, _) =>
            DefaultTypedDeploy.copy(mailbox = path)
          case unknown => throw new RuntimeException(s"Unsupported mailbox selector: $unknown")
        }

        val deployWithDispatcher = props.firstOrElse[DispatcherSelector](DispatcherDefault.empty) match {
          case _: DispatcherDefault          => deployWithMailbox
          case DispatcherFromConfig(name, _) => deployWithMailbox.copy(dispatcher = name)
          case _: DispatcherSameAsParent     => deployWithMailbox.copy(dispatcher = Deploy.DispatcherSameAsParent)
          case unknown                       => throw new RuntimeException(s"Unsupported dispatcher selector: $unknown")
        }

        val tags = props.firstOrElse[ActorTags](ActorTagsImpl.empty).tags
        val deployWithTags =
          if (tags.isEmpty) deployWithDispatcher
          else deployWithDispatcher.withTags(tags)

        if (deployWithTags.scope != LocalScope) // only replace if changed, withDeploy is expensive
          deployWithTags.copy(scope = Deploy.local.scope) // disallow remote deployment for typed actors
        else deployWithTags
      }

    // avoid the apply methods and also avoid copying props, for performance reasons
    new akka.actor.Props(
      deploy,
      TypedCreatorFunctionConsumerClazz,
      ActorAdapterClazz :: (() => new ActorAdapter(behavior(), rethrowTypedFailure)) :: Nil)
  }

}
