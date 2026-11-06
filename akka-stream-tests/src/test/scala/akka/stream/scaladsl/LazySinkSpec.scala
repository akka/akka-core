/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.stream._
import akka.stream.Attributes.Attribute
import akka.stream.scaladsl.AttributesSpec.AttributesSink
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils._

class LazySinkSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """) {

  import system.dispatcher
  val ex = TE("")
  case class MyAttribute() extends Attribute
  val myAttributes = Attributes(MyAttribute())

  "A LazySink" must {
    "provide attributes to inner sink" in {
      val attributes = Source
        .single(Done)
        .toMat(Sink.lazyFutureSink(() => Future(Sink.fromGraph(new AttributesSink()))))(Keep.right)
        .addAttributes(myAttributes)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute should contain(MyAttribute())
    }
  }

}
