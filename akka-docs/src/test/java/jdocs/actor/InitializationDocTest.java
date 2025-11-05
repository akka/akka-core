/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.FI;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.Optional;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitializationDocTest extends AbstractJavaTest {

  static ActorSystem system = null;

  @BeforeClass
  public static void beforeClass() {
    system = ActorSystem.create("InitializationDocTest");
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  public static class PreStartInitExample extends AbstractActor {

    @Override
    public Receive createReceive() {
      return AbstractActor.emptyBehavior();
    }

    // #preStartInit
    @Override
    public void preStart() {
      // Initialize children here
    }

    // Overriding postRestart to disable the call to preStart()
    // after restarts
    @Override
    public void postRestart(Throwable reason) {}

    // The default implementation of preRestart() stops all the children
    // of the actor. To opt-out from stopping the children, we
    // have to override preRestart()
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
      // Keep the call to postStop(), but no stopping of children
      postStop();
    }
    // #preStartInit

  }

  public static class MessageInitExample extends AbstractActor {
    private String initializeMe = null;

    // #messageInit
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "init",
              m1 -> {
                initializeMe = "Up and running";
                getContext()
                    .become(
                        receiveBuilder()
                            .matchEquals(
                                "U OK?",
                                m2 -> {
                                  getSender().tell(initializeMe, getSelf());
                                })
                            .build());
              })
          .build();
    }
    // #messageInit
  }

  public class GenericMessage<T> {
    T value;

    public GenericMessage(T value) {
      this.value = value;
    }
  }

  public static class GenericActor extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchUnchecked(
              GenericMessage.class,
              (GenericMessage<String> msg) -> {
                GenericMessage<String> message = msg;
                getSender().tell(message.value.toUpperCase(), getSelf());
              })
          .build();
    }
  }

  static class GenericActorWithPredicate extends AbstractActor {
    @Override
    public Receive createReceive() {
      FI.TypedPredicate<GenericMessage<String>> typedPredicate = s -> !s.value.isEmpty();

      return receiveBuilder()
          .matchUnchecked(
              GenericMessage.class,
              typedPredicate,
              (GenericMessage<String> msg) -> {
                getSender().tell(msg.value.toUpperCase(), getSelf());
              })
          .build();
    }
  }

  static class GenericActorWithPredicateAlwaysResponse extends AbstractActor {
    private boolean alwaysResponse() {
      return true;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchUnchecked(
              GenericMessage.class,
              this::alwaysResponse,
              (GenericMessage<String> msg) -> {
                getSender().tell(msg.value.toUpperCase(), getSelf());
              })
          .build();
    }
  }

  @Test
  public void testIt() {

    new TestKit(system) {
      {
        ActorRef testactor = system.actorOf(Props.create(MessageInitExample.class), "testactor");
        String msg = "U OK?";

        testactor.tell(msg, getRef());
        expectNoMessage(Duration.ofSeconds(1));

        testactor.tell("init", getRef());
        testactor.tell(msg, getRef());
        expectMsgEquals("Up and running");
      }
    };
  }

  @Test
  public void testGenericActor() {
    new TestKit(system) {
      {
        ActorRef genericTestActor =
            system.actorOf(Props.create(GenericActor.class), "genericActor");
        GenericMessage<String> genericMessage = new GenericMessage<String>("a");

        genericTestActor.tell(genericMessage, getRef());
        expectMsgEquals("A");
      }
    };
  }

  @Test
  public void actorShouldNotRespondForEmptyMessage() {
    new TestKit(system) {
      {
        ActorRef genericTestActor =
            system.actorOf(
                Props.create(GenericActorWithPredicate.class), "genericActorWithPredicate");
        GenericMessage<String> emptyGenericMessage = new GenericMessage<String>("");
        GenericMessage<String> nonEmptyGenericMessage = new GenericMessage<String>("a");

        genericTestActor.tell(emptyGenericMessage, getRef());
        expectNoMessage();

        genericTestActor.tell(nonEmptyGenericMessage, getRef());
        expectMsgEquals("A");
      }
    };
  }

  @Test
  public void actorShouldAlwaysRespondForEmptyMessage() {
    new TestKit(system) {
      {
        ActorRef genericTestActor =
            system.actorOf(
                Props.create(GenericActorWithPredicateAlwaysResponse.class),
                "genericActorWithPredicateAlwaysResponse");
        GenericMessage<String> emptyGenericMessage = new GenericMessage<String>("");
        GenericMessage<String> nonEmptyGenericMessage = new GenericMessage<String>("a");

        genericTestActor.tell(emptyGenericMessage, getRef());
        expectMsg("");

        genericTestActor.tell(nonEmptyGenericMessage, getRef());
        expectMsgEquals("A");
      }
    };
  }
}
