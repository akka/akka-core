/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.concurrent.duration._

import akka.actor.{ Actor, ActorSystem, Props }
import akka.stream.stage._
import akka.stream.scaladsl._
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MaterializerWithAttributesSpec
    extends TestKit(ActorSystem("MatWithAttributesSpec", MaterializerWithAttributesSpec.stackedConfig()))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer: Materializer = Materializer(system, Attributes.name("foo"))

  val attributesSource: GraphStageWithMaterializedValue[SourceShape[Nothing], Attributes] =
    new GraphStageWithMaterializedValue[SourceShape[Nothing], Attributes] {
      val out = Outlet[Nothing]("AttributesSource.out")
      override val shape = SourceShape(out)

      override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Attributes) =
        new GraphStageLogic(shape) with OutHandler {
          override def preStart(): Unit = completeStage()
          override def onPull(): Unit = completeStage()

          setHandler(out, this)
        } -> inheritedAttributes
    }

  "SystemMaterializer with attributes" should {
    "apply its attributes to streams it materializes" in {
      val attributes = Source.fromGraph(attributesSource).to(Sink.ignore).run()
      attributes.get[Attributes.Name].map(_.n) should contain("foo")

      Source
        .fromGraph(attributesSource)
        .to(Sink.ignore)
        .run()(Materializer(system))
        .get[Attributes.Name]
        .map(_.n) shouldNot contain("foo")
    }
  }

  class StreamActor extends Actor {
    def receive = {
      case "do it!" =>
        implicit val materializer = Materializer(context, Attributes.name("bar"))

        val firstAttributes = Source.fromGraph(attributesSource).to(Sink.ignore).run()
        val secondAttributes = Source.fromGraph(attributesSource).to(Sink.ignore).run()(Materializer(context))

        sender() ! firstAttributes
        sender() ! secondAttributes

        context.stop(self)
    }
  }

  val props = Props(new StreamActor).withMailbox("unbounded")

  "ActorMaterializer with attributes" should {
    "apply its attributes to streams it materializes" in {
      val streamActor = system.actorOf(props)

      streamActor ! "do it!"

      val responses = receiveN(2, 2.seconds)
      responses.filterNot(_.isInstanceOf[Attributes]) shouldBe empty

      val soManyAttributes = responses.map(_.asInstanceOf[Attributes])
      soManyAttributes.head.get[Attributes.Name].map(_.n) should contain("bar")
      soManyAttributes.last.get[Attributes.Name].map(_.n) shouldNot contain("bar")
    }
  }
}

object MaterializerWithAttributesSpec {
  val mailboxConfig =
    ConfigFactory.parseString("""unbounded { mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox" }""")

  def stackedConfig(): Config =
    mailboxConfig
      .withFallback(ConfigFactory.defaultApplication())
      .withFallback(ConfigFactory.defaultReferenceUnresolved())
      .resolve()
}
