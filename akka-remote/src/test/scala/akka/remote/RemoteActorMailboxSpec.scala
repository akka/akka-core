/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import com.typesafe.config.ConfigFactory

import akka.actor.ActorMailboxSpec

class RemoteActorMailboxSpec
    extends ActorMailboxSpec(
      ConfigFactory.parseString("""akka.actor.provider = remote""").withFallback(ActorMailboxSpec.mailboxConf)) {}
