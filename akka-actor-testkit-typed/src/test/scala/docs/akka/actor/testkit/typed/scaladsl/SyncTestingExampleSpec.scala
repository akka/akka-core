/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.actor.testkit.typed.scaladsl

//#imports
import akka.actor.testkit.typed.CapturedLogEvent
import akka.actor.testkit.typed.Effect._
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import com.typesafe.config.ConfigFactory
import org.slf4j.event.Level

//#imports
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object SyncTestingExampleSpec {
  //#child
  val childActor = Behaviors.receiveMessage[String] { _ =>
    Behaviors.same[String]
  }
  //#child

  //#under-test
  object Hello {
    sealed trait Command
    case object CreateAnonymousChild extends Command
    case class CreateChild(childName: String) extends Command
    case class SayHelloToChild(childName: String) extends Command
    case object SayHelloToAnonymousChild extends Command
    case class SayHello(who: ActorRef[String]) extends Command
    case class LogAndSayHello(who: ActorRef[String]) extends Command
    case class AskAQuestion(who: ActorRef[Question]) extends Command
    case class GotAnAnswer(answer: String, from: ActorRef[Question]) extends Command
    case class NoAnswerFrom(whom: ActorRef[Question]) extends Command

    def apply(): Behaviors.Receive[Command] = Behaviors.receivePartial {
      case (context, CreateChild(name)) =>
        context.spawn(childActor, name)
        Behaviors.same
      case (context, CreateAnonymousChild) =>
        context.spawnAnonymous(childActor)
        Behaviors.same
      case (context, SayHelloToChild(childName)) =>
        val child: ActorRef[String] = context.spawn(childActor, childName)
        child ! "hello"
        Behaviors.same
      case (context, SayHelloToAnonymousChild) =>
        val child: ActorRef[String] = context.spawnAnonymous(childActor)
        child ! "hello stranger"
        Behaviors.same
      case (_, SayHello(who)) =>
        who ! "hello"
        Behaviors.same
      case (context, LogAndSayHello(who)) =>
        context.log.info("Saying hello to {}", who.path.name)
        who ! "hello"
        Behaviors.same
      case (context, AskAQuestion(who)) =>
        implicit val timeout: Timeout = 10.seconds
        context.ask[Question, Answer](who, Question("do you know who I am?", _)) {
          case Success(answer) => GotAnAnswer(answer.a, who)
          case Failure(_)      => NoAnswerFrom(who)
        }
        Behaviors.same
      case (context, GotAnAnswer(answer, from)) =>
        context.log.info("Got an answer [{}] from {}", answer, from)
        Behaviors.same
      case (context, NoAnswerFrom(from)) =>
        context.log.info("Did not get an answer from {}", from)
        Behaviors.same
    }

    // Included in Hello for brevity
    case class Question(q: String, replyTo: ActorRef[Answer])
    case class Answer(a: String)
  }
  //#under-test

  object ConfigAware {
    sealed trait Command
    case class GetCfgString(key: String, replyTo: ActorRef[String]) extends Command
    case class SpawnChild(replyTo: ActorRef[ActorRef[Command]]) extends Command

    def apply(): Behavior[Command] = Behaviors.setup[Command] { ctx =>
      Behaviors.receiveMessage {
        case GetCfgString(key, replyTo) =>
          val str = ctx.system.settings.config.getString(key)
          replyTo ! str
          Behaviors.same
        case SpawnChild(replyTo) =>
          val child = ctx.spawnAnonymous(ConfigAware())
          replyTo ! child
          Behaviors.same
      }
    }
  }

}

class SyncTestingExampleSpec extends AnyWordSpec with Matchers {

  import SyncTestingExampleSpec._

  "Typed actor synchronous testing" must {

    "record spawning" in {
      //#test-child
      val testKit = BehaviorTestKit(Hello())
      testKit.run(Hello.CreateChild("child"))
      testKit.expectEffect(Spawned(childActor, "child"))
      //#test-child
    }

    "record spawning anonymous" in {
      //#test-anonymous-child
      val testKit = BehaviorTestKit(Hello())
      testKit.run(Hello.CreateAnonymousChild)
      testKit.expectEffect(SpawnedAnonymous(childActor))
      //#test-anonymous-child
    }

    "record message sends" in {
      //#test-message
      val testKit = BehaviorTestKit(Hello())
      val inbox = TestInbox[String]()
      testKit.run(Hello.SayHello(inbox.ref))
      inbox.expectMessage("hello")
      //#test-message
    }

    "send a message to a spawned child" in {
      //#test-child-message
      val testKit = BehaviorTestKit(Hello())
      testKit.run(Hello.SayHelloToChild("child"))
      val childInbox = testKit.childInbox[String]("child")
      childInbox.expectMessage("hello")
      //#test-child-message
    }

    "send a message to an anonymous spawned child" in {
      //#test-child-message-anonymous
      val testKit = BehaviorTestKit(Hello())
      testKit.run(Hello.SayHelloToAnonymousChild)
      val child = testKit.expectEffectType[SpawnedAnonymous[String]]

      val childInbox = testKit.childInbox(child.ref)
      childInbox.expectMessage("hello stranger")
      //#test-child-message-anonymous
    }

    "log a message to the logger" in {
      //#test-check-logging
      val testKit = BehaviorTestKit(Hello())
      val inbox = TestInbox[String]("Inboxer")
      testKit.run(Hello.LogAndSayHello(inbox.ref))
      testKit.logEntries() shouldBe Seq(CapturedLogEvent(Level.INFO, "Saying hello to Inboxer"))
      //#test-check-logging
    }

    "support the contextual ask pattern" in {
      //#test-contextual-ask
      val testKit = BehaviorTestKit(Hello())
      val askee = TestInbox[Hello.Question]()
      testKit.run(Hello.AskAQuestion(askee.ref))

      // The ask message is sent and can be inspected via the TestInbox
      // note that the "replyTo" address is not directly predictable
      val question = askee.receiveMessage()

      // The particulars of the `context.ask` call are captured as an Effect
      val effect = testKit.expectEffectType[AskInitiated[Hello.Question, Hello.Answer, Hello.Command]]

      testKit.clearLog()

      // The returned effect can be used to complete or time-out the ask at most once
      effect.respondWith(Hello.Answer("I think I met you somewhere, sometime"))
      // (since we completed the ask, timing out is commented out)
      // effect.timeout()

      // Completing/timing-out the ask is processed synchronously
      testKit.logEntries().size shouldBe 1

      // The message (including the synthesized "replyTo" address) can be inspected from the effect
      val sentQuestion = effect.askMessage

      // The response adaptation can be tested as many times as you want without completing the ask
      val response1 = effect.adaptResponse(Hello.Answer("No.  Who are you?"))
      val response2 = effect.adaptResponse(Hello.Answer("Hey Joe!"))

      // ... as can the message sent on a timeout
      val timeoutResponse = effect.adaptTimeout

      // The response timeout can be inspected
      val responseTimeout = effect.responseTimeout
      //#test-contextual-ask

      // pro-forma assertions to satisfy warn-unused while following the pattern in this spec of not
      // using ScalaTest matchers in code exposed through paradox
      question shouldNot be(null)
      sentQuestion shouldNot be(null)
      response1 shouldNot be(null)
      response2 shouldNot be(null)
      timeoutResponse shouldNot be(null)
      responseTimeout shouldNot be(null)
    }

    "has access to the provided config" in {
      val conf =
        BehaviorTestKit.ApplicationTestConfig.withFallback(ConfigFactory.parseString("test.secret=shhhhh"))
      val testKit = BehaviorTestKit(ConfigAware(), "root", conf)
      val inbox = TestInbox[AnyRef]("Inboxer")
      testKit.run(ConfigAware.GetCfgString("test.secret", inbox.ref.narrow))
      inbox.expectMessage("shhhhh")

      testKit.run(ConfigAware.SpawnChild(inbox.ref.narrow))
      val childTestKit = inbox.receiveMessage() match {
        case ar: ActorRef[_] =>
          testKit.childTestKit(ar.unsafeUpcast[Any].narrow[ConfigAware.Command])
        case unexpected =>
          unexpected should be(a[ActorRef[_]])
          ???
      }

      childTestKit.run(ConfigAware.GetCfgString("test.secret", inbox.ref.narrow))
      inbox.expectMessage("shhhhh")
    }
  }
}
