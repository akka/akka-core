/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.stream.testkit._

import scala.concurrent.duration._
import akka.testkit.TestLatch

import scala.annotation.nowarn
import scala.concurrent.Await

@nowarn("msg=never used") // sample snippets
class RecipeSimpleDrop extends RecipeSpec {

  "Recipe for simply dropping elements for a faster stream" must {

    "work" in {

      //#simple-drop
      val droppyStream: Flow[Message, Message, NotUsed] =
        Flow[Message].conflate((lastMessage, newMessage) => newMessage)
      //#simple-drop
      val latch = TestLatch(2)
      val realDroppyStream =
        Flow[Message].conflate((lastMessage, newMessage) => { latch.countDown(); newMessage })

      val pub = TestPublisher.probe[Message]()
      val sub = TestSubscriber.manualProbe[Message]()
      val messageSource = Source.fromPublisher(pub)
      val sink = Sink.fromSubscriber(sub)

      messageSource.via(realDroppyStream).to(sink).run()

      val subscription = sub.expectSubscription()
      sub.expectNoMessage()

      pub.sendNext("1")
      pub.sendNext("2")
      pub.sendNext("3")

      Await.ready(latch, 1.second)

      subscription.request(1)
      sub.expectNext("3")

      pub.sendComplete()
      subscription.request(1)
      sub.expectComplete()
    }

  }

}
