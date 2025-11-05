/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor._

/**
 * A spec that verified that the AkkaException has at least a single argument constructor of type String.
 *
 * This is required to make Akka Exceptions be friends with serialization/deserialization.
 */
class AkkaExceptionSpec extends AnyWordSpec with Matchers {

  "AkkaException" must {
    "have a AkkaException(String msg) constructor to be serialization friendly" in {
      //if the call to this method completes, we know what there is at least a single constructor which has
      //the expected argument type.
      verify(classOf[AkkaException])

      //lets also try it for the exception that triggered this bug to be discovered.
      verify(classOf[ActorKilledException])
    }
  }

  def verify(clazz: java.lang.Class[_]): Unit = {
    clazz.getConstructor(Array(classOf[String]): _*)
  }
}
