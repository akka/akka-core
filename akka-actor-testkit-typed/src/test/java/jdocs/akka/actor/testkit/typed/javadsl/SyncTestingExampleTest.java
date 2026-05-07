/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.actor.testkit.typed.javadsl;

// #imports
import akka.actor.testkit.typed.CapturedLogEvent;
import akka.actor.testkit.typed.Effect;
import akka.actor.testkit.typed.javadsl.BehaviorTestKit;
import akka.actor.testkit.typed.javadsl.TestInbox;
import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.time.Duration;

import com.typesafe.config.Config;
import org.slf4j.event.Level;
// #imports
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.scalatestplus.junit.JUnitSuite;

public class SyncTestingExampleTest extends JUnitSuite {

  // #child
  public static class Child {
    public static Behavior<String> create() {
      return Behaviors.receive((context, message) -> Behaviors.same());
    }
  }
  // #child

  // #under-test
  public static class Hello extends AbstractBehavior<Hello.Command> {

    public interface Command {}

    public static class CreateAChild implements Command {
      public final String childName;

      public CreateAChild(String childName) {
        this.childName = childName;
      }
    }

    public enum CreateAnAnonymousChild implements Command {
      INSTANCE
    }

    public static class SayHelloToChild implements Command {
      public final String childName;

      public SayHelloToChild(String childName) {
        this.childName = childName;
      }
    }

    public enum SayHelloToAnonymousChild implements Command {
      INSTANCE
    }

    public static class SayHello implements Command {
      public final ActorRef<String> who;

      public SayHello(ActorRef<String> who) {
        this.who = who;
      }
    }

    public static class LogAndSayHello implements Command {
      public final ActorRef<String> who;

      public LogAndSayHello(ActorRef<String> who) {
        this.who = who;
      }
    }

    public static class AskAQuestion implements Command {
      public final ActorRef<Question> who;

      public AskAQuestion(ActorRef<Question> who) {
        this.who = who;
      }
    }

    public static class GotAnAnswer implements Command {
      public final String answer;
      public final ActorRef<Question> from;

      public GotAnAnswer(String answer, ActorRef<Question> from) {
        this.answer = answer;
        this.from = from;
      }
    }

    public static class NoAnswerFrom implements Command {
      public final ActorRef<Question> whom;

      public NoAnswerFrom(ActorRef<Question> whom) {
        this.whom = whom;
      }
    }

    public static class Question {
      public final String q;
      public final ActorRef<Answer> replyTo;

      public Question(String q, ActorRef<Answer> replyTo) {
        this.q = q;
        this.replyTo = replyTo;
      }
    }

    public static class Answer {
      public final String a;

      public Answer(String a) {
        this.a = a;
      }
    }

    public static Behavior<Command> create() {
      return Behaviors.setup(Hello::new);
    }

    private Hello(ActorContext<Command> context) {
      super(context);
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(CreateAChild.class, this::onCreateAChild)
          .onMessage(CreateAnAnonymousChild.class, this::onCreateAnonymousChild)
          .onMessage(SayHelloToChild.class, this::onSayHelloToChild)
          .onMessage(SayHelloToAnonymousChild.class, this::onSayHelloToAnonymousChild)
          .onMessage(SayHello.class, this::onSayHello)
          .onMessage(LogAndSayHello.class, this::onLogAndSayHello)
          .onMessage(AskAQuestion.class, this::onAskAQuestion)
          .onMessage(GotAnAnswer.class, this::onGotAnAnswer)
          .onMessage(NoAnswerFrom.class, this::onNoAnswerFrom)
          .build();
    }

    private Behavior<Command> onCreateAChild(CreateAChild message) {
      getContext().spawn(Child.create(), message.childName);
      return Behaviors.same();
    }

    private Behavior<Command> onCreateAnonymousChild(CreateAnAnonymousChild message) {
      getContext().spawnAnonymous(Child.create());
      return Behaviors.same();
    }

    private Behavior<Command> onSayHelloToChild(SayHelloToChild message) {
      ActorRef<String> child = getContext().spawn(Child.create(), message.childName);
      child.tell("hello");
      return Behaviors.same();
    }

    private Behavior<Command> onSayHelloToAnonymousChild(SayHelloToAnonymousChild message) {
      ActorRef<String> child = getContext().spawnAnonymous(Child.create());
      child.tell("hello stranger");
      return Behaviors.same();
    }

    private Behavior<Command> onSayHello(SayHello message) {
      message.who.tell("hello");
      return Behaviors.same();
    }

    private Behavior<Command> onLogAndSayHello(LogAndSayHello message) {
      getContext().getLog().info("Saying hello to {}", message.who.path().name());
      message.who.tell("hello");
      return Behaviors.same();
    }

    private Behavior<Command> onAskAQuestion(AskAQuestion message) {
      getContext()
          .ask(
              Answer.class,
              message.who,
              Duration.ofSeconds(10),
              (ActorRef<Answer> ref) -> new Question("do you know who I am?", ref),
              (response, throwable) -> {
                if (response != null) {
                  return new GotAnAnswer(response.a, message.who);
                } else {
                  return new NoAnswerFrom(message.who);
                }
              });
      return Behaviors.same();
    }

    private Behavior<Command> onGotAnAnswer(GotAnAnswer message) {
      getContext().getLog().info("Got an answer[{}] from {}", message.answer, message.from);
      return Behaviors.same();
    }

    private Behavior<Command> onNoAnswerFrom(NoAnswerFrom message) {
      getContext().getLog().info("Did not get an answer from {}", message.whom);
      return Behaviors.same();
    }
  }
  // #under-test

  @Test
  public void testSpawning() {
    // #test-child
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create());
    test.run(new Hello.CreateAChild("child"));
    assertEquals("child", test.expectEffectClass(Effect.Spawned.class).childName());
    // #test-child
  }

  @Test
  public void testSpawningAnonymous() {
    // #test-anonymous-child
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create());
    test.run(Hello.CreateAnAnonymousChild.INSTANCE);
    test.expectEffectClass(Effect.SpawnedAnonymous.class);
    // #test-anonymous-child
  }

  @Test
  public void testRecodingMessageSend() {
    // #test-message
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create());
    TestInbox<String> inbox = TestInbox.create();
    test.run(new Hello.SayHello(inbox.getRef()));
    inbox.expectMessage("hello");
    // #test-message
  }

  @Test
  public void testMessageToChild() {
    // #test-child-message
    BehaviorTestKit<Hello.Command> testKit = BehaviorTestKit.create(Hello.create());
    testKit.run(new Hello.SayHelloToChild("child"));
    TestInbox<String> childInbox = testKit.childInbox("child");
    childInbox.expectMessage("hello");
    // #test-child-message
  }

  @Test
  public void testMessageToAnonymousChild() {
    // #test-child-message-anonymous
    BehaviorTestKit<Hello.Command> testKit = BehaviorTestKit.create(Hello.create());
    testKit.run(Hello.SayHelloToAnonymousChild.INSTANCE);
    // Anonymous actors are created as: $a $b etc
    TestInbox<String> childInbox = testKit.childInbox("$a");
    childInbox.expectMessage("hello stranger");
    // #test-child-message-anonymous
  }

  @Test
  public void testCheckLogging() {
    // #test-check-logging
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create());
    TestInbox<String> inbox = TestInbox.create("Inboxer");
    test.run(new Hello.LogAndSayHello(inbox.getRef()));

    List<CapturedLogEvent> allLogEntries = test.getAllLogEntries();
    assertEquals(1, allLogEntries.size());
    CapturedLogEvent expectedLogEvent =
        new CapturedLogEvent(
            Level.INFO,
            "Saying hello to Inboxer",
            Optional.empty(),
            Optional.empty(),
            new HashMap<>());
    assertEquals(expectedLogEvent, allLogEntries.get(0));
    // #test-check-logging
  }

  @Test
  public void testSupportContextualAsk() {
    // #test-contextual-ask
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create());
    TestInbox<Hello.Question> askee = TestInbox.create();
    test.run(new Hello.AskAQuestion(askee.getRef()));

    Hello.Question question = askee.receiveMessage();
    // Note that the replyTo address in the message is not a priori predictable, so shouldn't be
    // asserted against
    assertEquals(question.q, "do you know who I am?");

    // Retrieve a description of the performed ask
    @SuppressWarnings("unchecked")
    Effect.AskInitiated<Hello.Question, Hello.Answer, Hello.Command> effect =
        test.expectEffectClass(Effect.AskInitiated.class);

    test.clearLog();

    // The effect can be used to complete or time-out the ask at most once
    effect.respondWith(new Hello.Answer("I think I met you somewhere, sometime"));
    // commented out because we've completed the ask
    // effect.timeout();

    // Completing/timing-out the ask is processed synchronously
    List<CapturedLogEvent> allLogEntries = test.getAllLogEntries();
    assertEquals(allLogEntries.size(), 1);

    // The message, including the synthesized "replyTo", can be inspected from the effect
    assertEquals(question, effect.askMessage());

    // The response adaptation can be tested as many times as you want without completing the ask
    Hello.Command response1 = effect.adaptResponse(new Hello.Answer("No.  Who are you?"));
    assertEquals(((Hello.GotAnAnswer) response1).answer, "No.  Who are you?");

    // ... as can the message sent on a timeout
    assertTrue(effect.adaptTimeout() instanceof Hello.NoAnswerFrom);

    // The response timeout is captured
    assertEquals(effect.responseTimeout().toSeconds(), 10L);
    // #test-contextual-ask
  }

  @Test
  public void testWithAppTestCfg() {

    // #test-app-test-config
    Config cfg = BehaviorTestKit.applicationTestConfig();
    BehaviorTestKit<Hello.Command> test = BehaviorTestKit.create(Hello.create(), "hello", cfg);
  }
}
