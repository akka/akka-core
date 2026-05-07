/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.jdk.CollectionConverters._

/**
 * The journal may support tagging of events that are used by the
 * `EventsByTag` query and it may support specifying the tags via an
 * [[akka.persistence.journal.EventAdapter]] that wraps the events
 * in a `Tagged` with the given `tags`. The journal may support other
 * ways of doing tagging. Please consult the documentation of the specific
 * journal implementation for more information.
 *
 * The journal will unwrap the event and store the `payload`.
 */
case class Tagged(payload: Any, tags: Set[String]) {

  /**
   * Java API
   */
  def this(payload: Any, tags: java.util.Set[String]) = {
    this(payload, tags.asScala.toSet)
  }
}
