/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor.NoSerializationVerificationNeeded

/**
 * INTERNAL API.
 *
 * Messages exchanged between persistent actors and a journal/snapshot-store.
 */
private[persistence] object Protocol {

  /**
   * INTERNAL API.
   *
   * Internal persistence extension messages extend this trait.
   *
   * Helps persistence plugin developers to differentiate
   * internal persistence extension messages from their custom plugin messages.
   *
   * Journal messages need not be serialization verified as the Journal Actor
   * should always be a local Actor (and serialization is performed by plugins).
   * One notable exception to this is the shared journal used for testing.
   */
  trait Message extends NoSerializationVerificationNeeded

}
