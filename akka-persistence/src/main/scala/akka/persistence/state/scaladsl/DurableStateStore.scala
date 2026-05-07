/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.state.scaladsl

import scala.jdk.OptionConverters._
import scala.concurrent.Future

import akka.persistence.state.javadsl.{ GetObjectResult => JGetObjectResult }

/**
 * API for reading durable state objects with payload `A`.
 *
 * For Java API see [[akka.persistence.state.javadsl.DurableStateStore]].
 *
 * See also [[DurableStateUpdateStore]]
 */
trait DurableStateStore[A] {

  def getObject(persistenceId: String): Future[GetObjectResult[A]]

}

final case class GetObjectResult[A](value: Option[A], revision: Long) {
  def toJava: JGetObjectResult[A] = JGetObjectResult(value.toJava, revision)
}
