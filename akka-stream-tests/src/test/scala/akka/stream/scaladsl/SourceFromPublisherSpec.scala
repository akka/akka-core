/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.testkit.TestPublisher
import akka.testkit.TestKit

class SourceFromPublisherSpec
    extends TestKit(ActorSystem("source-from-publisher-spec"))
    with AsyncWordSpecLike
    with Matchers {

  "Source.fromPublisher" should {
    // https://github.com/akka/akka/pull/31129
    "consider 'inputBuffer' attributes in a correct way" in pendingUntilFixed {
      val publisher = TestPublisher.probe[Int]()
      Source.fromPublisher(publisher).addAttributes(Attributes.inputBuffer(1, 2)).runWith(Sink.ignore)
      publisher.expectRequest() should ===(2L)
    }
  }
}
