/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

import akka.actor.*;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jdocs.AbstractJavaTest;
import static jdocs.actor.Messages.Swap.Swap;
import static jdocs.actor.Messages.*;
import akka.actor.CoordinatedShutdown;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import akka.testkit.TestActors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

// #import-props
import akka.actor.Props;
// #import-props
// #import-actorRef
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
// #import-actorRef
// #import-identify
import akka.actor.ActorIdentity;
import akka.actor.ActorSelection;
import akka.actor.Identify;
// #import-identify
// #import-ask
import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

import java.util.concurrent.CompletableFuture;
// #import-ask
// #import-gracefulStop
import static akka.pattern.Patterns.gracefulStop;
import akka.pattern.AskTimeoutException;
import java.util.concurrent.CompletionStage;

// #import-gracefulStop
// #import-terminated
import akka.actor.Terminated;
// #import-terminated

public class ActorDocTest extends AbstractJavaTest {

  public static Config config =
      ConfigFactory.parseString(
          "akka {\n"
              + "  loggers = [\"akka.testkit.TestEventListener\"]\n"
              + "  loglevel = \"WARNING\"\n"
              + "  stdout-loglevel = \"WARNING\"\n"
              + "}\n");

  static ActorSystem system = null;

  @BeforeClass
  public static void beforeClass() {
    system = ActorSystem.create("ActorDocTest", config);
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  public
  // #context-actorOf
  static class FirstActor extends AbstractActor {
    final ActorRef child = getContext().actorOf(Props.create(MyActor.class), "myChild");

    // #plus-some-behavior
    @Override
    public Receive createReceive() {
      return receiveBuilder().matchAny(x -> getSender().tell(x, getSelf())).build();
    }
    // #plus-some-behavior
  }
  // #context-actorOf

  public static class SomeActor extends AbstractActor {
    // #createReceive
    @Override
    public Receive createReceive() {
      return receiveBuilder().match(String.class, s -> System.out.println(s.toLowerCase())).build();
    }
    // #createReceive
  }

  public
  // #well-structured
  static class WellStructuredActor extends AbstractActor {

    public static class Msg1 {}

    public static class Msg2 {}

    public static class Msg3 {}

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(Msg1.class, this::receiveMsg1)
          .match(Msg2.class, this::receiveMsg2)
          .match(Msg3.class, this::receiveMsg3)
          .build();
    }

    private void receiveMsg1(Msg1 msg) {
      // actual work
    }

    private void receiveMsg2(Msg2 msg) {
      // actual work
    }

    private void receiveMsg3(Msg3 msg) {
      // actual work
    }
  }
  // #well-structured

  public
  // #optimized
  static class OptimizedActor extends UntypedAbstractActor {

    public static class Msg1 {}

    public static class Msg2 {}

    public static class Msg3 {}

    @Override
    public void onReceive(Object msg) throws Exception {
      if (msg instanceof Msg1) receiveMsg1((Msg1) msg);
      else if (msg instanceof Msg2) receiveMsg2((Msg2) msg);
      else if (msg instanceof Msg3) receiveMsg3((Msg3) msg);
      else unhandled(msg);
    }

    private void receiveMsg1(Msg1 msg) {
      // actual work
    }

    private void receiveMsg2(Msg2 msg) {
      // actual work
    }

    private void receiveMsg3(Msg3 msg) {
      // actual work
    }
  }
  // #optimized

  public static class ActorWithArgs extends AbstractActor {
    private final String args;

    public ActorWithArgs(String args) {
      this.args = args;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder().matchAny(x -> {}).build();
    }
  }

  public
  // #props-factory
  static class DemoActor extends AbstractActor {
    /**
     * Create Props for an actor of this type.
     *
     * @param magicNumber The magic number to be passed to this actor’s constructor.
     * @return a Props for creating this actor, which can then be further configured (e.g. calling
     *     `.withDispatcher()` on it)
     */
    static Props props(Integer magicNumber) {
      // You need to specify the actual type of the returned actor
      // since Java 8 lambdas have some runtime type information erased
      return Props.create(DemoActor.class, () -> new DemoActor(magicNumber));
    }

    private final Integer magicNumber;

    public DemoActor(Integer magicNumber) {
      this.magicNumber = magicNumber;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Integer.class,
              i -> {
                getSender().tell(i + magicNumber, getSelf());
              })
          .build();
    }
  }

  // #props-factory
  public
  // #props-factory
  static class SomeOtherActor extends AbstractActor {
    // Props(new DemoActor(42)) would not be safe
    ActorRef demoActor = getContext().actorOf(DemoActor.props(42), "demo");
    // ...
    // #props-factory
    @Override
    public Receive createReceive() {
      return AbstractActor.emptyBehavior();
    }
    // #props-factory
  }
  // #props-factory

  public
  // #messages-in-companion
  static class DemoMessagesActor extends AbstractLoggingActor {

    public static class Greeting {
      private final String from;

      public Greeting(String from) {
        this.from = from;
      }

      public String getGreeter() {
        return from;
      }
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Greeting.class,
              g -> {
                log().info("I was greeted by {}", g.getGreeter());
              })
          .build();
    }
  }
  // #messages-in-companion

  public static class LifecycleMethods extends AbstractActor {

    @Override
    public Receive createReceive() {
      return AbstractActor.emptyBehavior();
    }

    /*
     * This section must be kept in sync with the actual Actor trait.
     *
     * BOYSCOUT RULE: whenever you read this, verify that!
     */
    // #lifecycle-callbacks
    public void preStart() {}

    public void preRestart(Throwable reason, Optional<Object> message) {
      for (ActorRef each : getContext().getChildren()) {
        getContext().unwatch(each);
        getContext().stop(each);
      }
      postStop();
    }

    public void postRestart(Throwable reason) {
      preStart();
    }

    public void postStop() {}
    // #lifecycle-callbacks

  }

  public static class Hook extends AbstractActor {
    ActorRef target = null;

    @Override
    public Receive createReceive() {
      return AbstractActor.emptyBehavior();
    }

    // #preStart
    @Override
    public void preStart() {
      target = getContext().actorOf(Props.create(MyActor.class, "target"));
    }
    // #preStart
    // #postStop
    @Override
    public void postStop() {
      // #clean-up-some-resources
      final String message = "stopped";
      // #tell
      // don’t forget to think about who is the sender (2nd argument)
      target.tell(message, getSelf());
      // #tell
      final Object result = "";
      // #forward
      target.forward(result, getContext());
      // #forward
      target = null;
      // #clean-up-some-resources
    }
    // #postStop

    // compilation test only
    public void compileSelections() {
      // #selection-local
      // will look up this absolute path
      getContext().actorSelection("/user/serviceA/actor");
      // will look up sibling beneath same supervisor
      getContext().actorSelection("../joe");
      // #selection-local

      // #selection-wildcard
      // will look all children to serviceB with names starting with worker
      getContext().actorSelection("/user/serviceB/worker*");
      // will look up all siblings beneath same supervisor
      getContext().actorSelection("../*");
      // #selection-wildcard

      // #selection-remote
      getContext().actorSelection("akka://app@otherhost:1234/user/serviceB");
      // #selection-remote
    }
  }

  public static class ReplyException extends AbstractActor {

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchAny(
              x -> {
                // #reply-exception
                try {
                  String result = operation();
                  getSender().tell(result, getSelf());
                } catch (Exception e) {
                  getSender().tell(new akka.actor.Status.Failure(e), getSelf());
                  throw e;
                }
                // #reply-exception
              })
          .build();
    }

    private String operation() {
      return "Hi";
    }
  }

  public
  // #gracefulStop-actor
  static class Manager extends AbstractActor {
    private static enum Shutdown {
      Shutdown
    }

    public static final Shutdown SHUTDOWN = Shutdown.Shutdown;

    private ActorRef worker =
        getContext().watch(getContext().actorOf(Props.create(Cruncher.class), "worker"));

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "job",
              s -> {
                worker.tell("crunch", getSelf());
              })
          .matchEquals(
              SHUTDOWN,
              x -> {
                worker.tell(PoisonPill.getInstance(), getSelf());
                getContext().become(shuttingDown());
              })
          .build();
    }

    private AbstractActor.Receive shuttingDown() {
      return receiveBuilder()
          .matchEquals(
              "job", s -> getSender().tell("service unavailable, shutting down", getSelf()))
          .match(Terminated.class, t -> t.actor().equals(worker), t -> getContext().stop(getSelf()))
          .build();
    }
  }
  // #gracefulStop-actor

  @Test
  public void usePatternsGracefulStop() throws Exception {
    ActorRef actorRef = system.actorOf(Props.create(Manager.class));
    // #gracefulStop
    try {
      CompletionStage<Boolean> stopped =
          gracefulStop(actorRef, Duration.ofSeconds(5), Manager.SHUTDOWN);
      stopped.toCompletableFuture().get(6, TimeUnit.SECONDS);
      // the actor has been stopped
    } catch (AskTimeoutException e) {
      // the actor wasn't stopped within 5 seconds
    }
    // #gracefulStop
  }

  public static class Cruncher extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder().matchEquals("crunch", s -> {}).build();
    }
  }

  public
  // #swapper
  static class Swapper extends AbstractLoggingActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              Swap,
              s -> {
                log().info("Hi");
                getContext()
                    .become(
                        receiveBuilder()
                            .matchEquals(
                                Swap,
                                x -> {
                                  log().info("Ho");
                                  getContext()
                                      .unbecome(); // resets the latest 'become' (just for fun)
                                })
                            .build(),
                        false); // push on top instead of replace
              })
          .build();
    }
  }

  // #swapper
  public
  // #swapper
  static class SwapperApp {
    public static void main(String[] args) {
      ActorSystem system = ActorSystem.create("SwapperSystem");
      ActorRef swapper = system.actorOf(Props.create(Swapper.class), "swapper");
      swapper.tell(Swap, ActorRef.noSender()); // logs Hi
      swapper.tell(Swap, ActorRef.noSender()); // logs Ho
      swapper.tell(Swap, ActorRef.noSender()); // logs Hi
      swapper.tell(Swap, ActorRef.noSender()); // logs Ho
      swapper.tell(Swap, ActorRef.noSender()); // logs Hi
      swapper.tell(Swap, ActorRef.noSender()); // logs Ho
      system.terminate();
    }
  }
  // #swapper

  @Test
  public void creatingActorWithSystemActorOf() {
    // #system-actorOf
    // ActorSystem is a heavy object: create only one per application
    final ActorSystem system = ActorSystem.create("MySystem", config);
    final ActorRef myActor = system.actorOf(Props.create(MyActor.class), "myactor");
    // #system-actorOf
    try {
      new TestKit(system) {
        {
          myActor.tell("hello", getRef());
          expectMsgEquals("hello");
        }
      };
    } finally {
      TestKit.shutdownActorSystem(system);
    }
  }

  @Test
  public void creatingGraduallyBuiltActorWithSystemActorOf() {
    final ActorSystem system = ActorSystem.create("MySystem", config);
    final ActorRef actor =
        system.actorOf(Props.create(GraduallyBuiltActor.class), "graduallyBuiltActor");
    try {
      new TestKit(system) {
        {
          actor.tell("hello", getRef());
          expectMsgEquals("hello");
        }
      };
    } finally {
      TestKit.shutdownActorSystem(system);
    }
  }

  @Test
  public void creatingPropsConfig() {
    // #creating-props
    Props props1 = Props.create(MyActor.class);
    Props props2 =
        Props.create(ActorWithArgs.class, () -> new ActorWithArgs("arg")); // careful, see below
    Props props3 = Props.create(ActorWithArgs.class, "arg");
    // #creating-props

    // #creating-props-deprecated
    // NOT RECOMMENDED within another actor:
    // encourages to close over enclosing class
    Props props7 = Props.create(ActorWithArgs.class, () -> new ActorWithArgs("arg"));
    // #creating-props-deprecated
  }

  // Commented out because this 'Props.create' overload is now deprecated
  // @Test(expected = IllegalArgumentException.class)
  public void creatingPropsIllegal() {
    /*
    // #creating-props-illegal
    // This will throw an IllegalArgumentException since some runtime
    // type information of the lambda is erased.
    // Use Props.create(actorClass, Creator) instead.
    Props props = Props.create(() -> new ActorWithArgs("arg"));
    // #creating-props-illegal
    */
  }

  public
  // #receive-timeout
  static class ReceiveTimeoutActor extends AbstractActor {
    // #receive-timeout
    ActorRef target = getContext().getSystem().deadLetters();
    // #receive-timeout
    public ReceiveTimeoutActor() {
      // To set an initial delay
      getContext().setReceiveTimeout(Duration.ofSeconds(10));
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "Hello",
              s -> {
                // To set in a response to a message
                getContext().setReceiveTimeout(Duration.ofSeconds(1));
                // #receive-timeout
                target = getSender();
                target.tell("Hello world", getSelf());
                // #receive-timeout
              })
          .match(
              ReceiveTimeout.class,
              r -> {
                // To turn it off
                getContext().cancelReceiveTimeout();
                // #receive-timeout
                target.tell("timeout", getSelf());
                // #receive-timeout
              })
          .build();
    }
  }
  // #receive-timeout

  @Test
  public void using_receiveTimeout() {
    final ActorRef myActor = system.actorOf(Props.create(ReceiveTimeoutActor.class));
    new TestKit(system) {
      {
        myActor.tell("Hello", getRef());
        expectMsgEquals("Hello world");
        expectMsgEquals("timeout");
      }
    };
  }

  static class ReceiveTimeoutWithCancelActor extends AbstractActor {
    private ActorRef target = getContext().getSystem().deadLetters();

    public ReceiveTimeoutWithCancelActor() {
      getContext().setReceiveTimeout(Duration.ofSeconds(1));
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "Hello",
              s -> {
                target = getSender();
                target.tell("Hello world", getSelf());
                getContext().setReceiveTimeout(Duration.ofMillis(200));
              })
          .match(
              ReceiveTimeout.class,
              r -> {
                getContext().cancelReceiveTimeout();
                target.tell("timeout", getSelf());
              })
          .build();
    }
  }

  @Test
  public void cancel_receiveTimeout() {
    final ActorRef myActor = system.actorOf(Props.create(ReceiveTimeoutWithCancelActor.class));
    new TestKit(system) {
      {
        myActor.tell("Hello", getRef());
        expectMsgEquals("Hello world");
        expectMsgEquals("timeout");
        expectNoMessage(Duration.ofMillis(400));
      }
    };
  }

  public
  // #hot-swap-actor
  static class HotSwapActor extends AbstractActor {
    private AbstractActor.Receive angry;
    private AbstractActor.Receive happy;

    public HotSwapActor() {
      angry =
          receiveBuilder()
              .matchEquals(
                  "foo",
                  s -> {
                    getSender().tell("I am already angry?", getSelf());
                  })
              .matchEquals(
                  "bar",
                  s -> {
                    getContext().become(happy);
                  })
              .build();

      happy =
          receiveBuilder()
              .matchEquals(
                  "bar",
                  s -> {
                    getSender().tell("I am already happy :-)", getSelf());
                  })
              .matchEquals(
                  "foo",
                  s -> {
                    getContext().become(angry);
                  })
              .build();
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals("foo", s -> getContext().become(angry))
          .matchEquals("bar", s -> getContext().become(happy))
          .build();
    }
  }
  // #hot-swap-actor

  @Test
  public void using_hot_swap() {
    final ActorRef actor = system.actorOf(Props.create(HotSwapActor.class), "hot");
    new TestKit(system) {
      {
        actor.tell("foo", getRef());
        actor.tell("foo", getRef());
        expectMsgEquals("I am already angry?");
        actor.tell("bar", getRef());
        actor.tell("bar", getRef());
        expectMsgEquals("I am already happy :-)");
        actor.tell("foo", getRef());
        actor.tell("foo", getRef());
        expectMsgEquals("I am already angry?");
        expectNoMessage(Duration.ofSeconds(1));
      }
    };
  }

  public
  // #stash
  static class ActorWithProtocol extends AbstractActorWithStash {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "open",
              s -> {
                getContext()
                    .become(
                        receiveBuilder()
                            .matchEquals(
                                "write",
                                ws -> {
                                  /* do writing */
                                })
                            .matchEquals(
                                "close",
                                cs -> {
                                  unstashAll();
                                  getContext().unbecome();
                                })
                            .matchAny(msg -> stash())
                            .build(),
                        false);
              })
          .matchAny(msg -> stash())
          .build();
    }
  }
  // #stash

  @Test
  public void using_Stash() {
    final ActorRef actor = system.actorOf(Props.create(ActorWithProtocol.class), "stash");
  }

  public
  // #watch
  static class WatchActor extends AbstractActor {
    private final ActorRef child = getContext().actorOf(Props.empty(), "target");
    private ActorRef lastSender = system.deadLetters();

    public WatchActor() {
      getContext().watch(child); // <-- this is the only call needed for registration
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "kill",
              s -> {
                getContext().stop(child);
                lastSender = getSender();
              })
          .match(
              Terminated.class,
              t -> t.actor().equals(child),
              t -> {
                lastSender.tell("finished", getSelf());
              })
          .build();
    }
  }
  // #watch

  @Test
  public void using_watch() {
    ActorRef actor = system.actorOf(Props.create(WatchActor.class));

    new TestKit(system) {
      {
        actor.tell("kill", getRef());
        expectMsgEquals("finished");
      }
    };
  }

  public
  // #identify
  static class Follower extends AbstractActor {
    final Integer identifyId = 1;

    public Follower() {
      ActorSelection selection = getContext().actorSelection("/user/another");
      selection.tell(new Identify(identifyId), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              ActorIdentity.class,
              id -> id.getActorRef().isPresent(),
              id -> {
                ActorRef ref = id.getActorRef().get();
                getContext().watch(ref);
                getContext().become(active(ref));
              })
          .match(
              ActorIdentity.class,
              id -> !id.getActorRef().isPresent(),
              id -> {
                getContext().stop(getSelf());
              })
          .build();
    }

    final AbstractActor.Receive active(final ActorRef another) {
      return receiveBuilder()
          .match(
              Terminated.class, t -> t.actor().equals(another), t -> getContext().stop(getSelf()))
          .build();
    }
  }
  // #identify

  @Test
  public void using_Identify() {
    ActorRef a = system.actorOf(Props.empty());
    ActorRef b = system.actorOf(Props.create(Follower.class));

    new TestKit(system) {
      {
        watch(b);
        system.stop(a);
        assertEquals(expectMsgClass(Duration.ofSeconds(2), Terminated.class).actor(), b);
      }
    };
  }

  @Test
  public void usePatternsAskPipe() {
    new TestKit(system) {
      {
        ActorRef actorA = system.actorOf(TestActors.echoActorProps());
        ActorRef actorB = system.actorOf(TestActors.echoActorProps());
        ActorRef actorC = getRef();

        // #ask-pipe
        final Duration t = Duration.ofSeconds(5);

        // using 1000ms timeout
        CompletableFuture<Object> future1 =
            ask(actorA, "request", Duration.ofMillis(1000)).toCompletableFuture();

        // using timeout from above
        CompletableFuture<Object> future2 = ask(actorB, "another request", t).toCompletableFuture();

        CompletableFuture<Result> transformed =
            future1.thenCombine(future2, (x, s) -> new Result((String) x, (String) s));

        pipe(transformed, system.dispatcher()).to(actorC);
        // #ask-pipe

        expectMsgEquals(new Result("request", "another request"));
      }
    };
  }

  @Test
  public void useKill() {
    new TestKit(system) {
      {
        ActorRef victim = system.actorOf(TestActors.echoActorProps());
        watch(victim);
        // #kill
        victim.tell(akka.actor.Kill.getInstance(), ActorRef.noSender());

        // expecting the actor to indeed terminate:
        expectTerminated(Duration.ofSeconds(3), victim);
        // #kill
      }
    };
  }

  @Test
  public void usePoisonPill() {
    new TestKit(system) {
      {
        ActorRef victim = system.actorOf(TestActors.echoActorProps());
        watch(victim);
        // #poison-pill
        victim.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
        // #poison-pill
        expectTerminated(Duration.ofSeconds(3), victim);
      }
    };
  }

  @Test
  public void coordinatedShutdownActorTermination() {
    ActorRef someActor = system.actorOf(Props.create(FirstActor.class));
    someActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
    // https://github.com/akka/akka-core/issues/29056
    // #coordinated-shutdown-addActorTerminationTask
    CoordinatedShutdown.get(system)
        .addActorTerminationTask(
            CoordinatedShutdown.PhaseBeforeServiceUnbind(),
            "someTaskName",
            someActor,
            Optional.of("stop"));
    // #coordinated-shutdown-addActorTerminationTask
  }
}
