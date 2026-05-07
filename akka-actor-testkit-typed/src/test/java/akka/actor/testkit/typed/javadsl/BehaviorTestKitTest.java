/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl;

import static org.junit.Assert.*;

import akka.Done;
import akka.actor.testkit.typed.CapturedLogEvent;
import akka.actor.testkit.typed.Effect;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.Behaviors;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Ignore;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import org.slf4j.event.Level;

public class BehaviorTestKitTest extends JUnitSuite {

  public interface Command {}

  public static class SpawnWatchAndUnWatch implements Command {
    private final String name;

    public SpawnWatchAndUnWatch(String name) {
      this.name = name;
    }
  }

  public static class SpawnAndWatchWith implements Command {
    private final String name;

    public SpawnAndWatchWith(String name) {
      this.name = name;
    }
  }

  public static class SpawnSession implements Command {
    private final ActorRef<ActorRef<String>> replyTo;
    private final ActorRef<String> sessionHandler;

    public SpawnSession(ActorRef<ActorRef<String>> replyTo, ActorRef<String> sessionHandler) {
      this.replyTo = replyTo;
      this.sessionHandler = sessionHandler;
    }
  }

  public static class KillSession implements Command {
    private final ActorRef<String> session;
    private final ActorRef<Done> replyTo;

    public KillSession(ActorRef<String> session, ActorRef<Done> replyTo) {
      this.session = session;
      this.replyTo = replyTo;
    }
  }

  public static class CreateMessageAdapter implements Command {
    private final Class<Object> clazz;
    private final akka.japi.function.Function<Object, Command> f;

    @SuppressWarnings("unchecked")
    public CreateMessageAdapter(Class clazz, akka.japi.function.Function<Object, Command> f) {
      this.clazz = clazz;
      this.f = f;
    }
  }

  public static class SpawnChildren implements Command {
    private final int numberOfChildren;

    public SpawnChildren(int numberOfChildren) {
      this.numberOfChildren = numberOfChildren;
    }
  }

  public static class SpawnChildrenAnonymous implements Command {
    private final int numberOfChildren;

    public SpawnChildrenAnonymous(int numberOfChildren) {
      this.numberOfChildren = numberOfChildren;
    }
  }

  public static class SpawnChildrenWithProps implements Command {
    private final int numberOfChildren;
    private final Props props;

    public SpawnChildrenWithProps(int numberOfChildren, Props props) {
      this.numberOfChildren = numberOfChildren;
      this.props = props;
    }
  }

  public static class SpawnChildrenAnonymousWithProps implements Command {
    private final int numberOfChildren;
    private final Props props;

    public SpawnChildrenAnonymousWithProps(int numberOfChildren, Props props) {
      this.numberOfChildren = numberOfChildren;
      this.props = props;
    }
  }

  public static class Log implements Command {
    private final String what;

    public Log(String what) {
      this.what = what;
    }
  }

  public static class AskForCookiesFrom implements Command {
    private final ActorRef<CookieDistributorCommand> distributor;

    public AskForCookiesFrom(ActorRef<CookieDistributorCommand> distributor) {
      this.distributor = distributor;
    }
  }

  public interface CookieDistributorCommand {}

  public static class GiveMeCookies implements CookieDistributorCommand {
    public final int nrCookies;
    public final ActorRef<CookiesForYou> replyTo;

    public GiveMeCookies(int nrCookies, ActorRef<CookiesForYou> replyTo) {
      this.nrCookies = nrCookies;
      this.replyTo = replyTo;
    }
  }

  public static class CookiesForYou {
    public final int nrCookies;

    public CookiesForYou(int nrCookies) {
      this.nrCookies = nrCookies;
    }
  }

  public interface Action {}

  private static Behavior<Action> childInitial = Behaviors.ignore();

  private static Props props = Props.empty().withDispatcherFromConfig("cat");

  private static Behavior<Command> behavior =
      Behaviors.setup(
          context -> {
            return Behaviors.receive(Command.class)
                .onMessage(
                    SpawnChildren.class,
                    message -> {
                      IntStream.range(0, message.numberOfChildren)
                          .forEach(
                              i -> {
                                context.spawn(childInitial, "child" + i);
                              });
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnChildrenAnonymous.class,
                    message -> {
                      IntStream.range(0, message.numberOfChildren)
                          .forEach(
                              i -> {
                                context.spawnAnonymous(childInitial);
                              });
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnChildrenWithProps.class,
                    message -> {
                      IntStream.range(0, message.numberOfChildren)
                          .forEach(
                              i -> {
                                context.spawn(childInitial, "child" + i, message.props);
                              });
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnChildrenAnonymousWithProps.class,
                    message -> {
                      IntStream.range(0, message.numberOfChildren)
                          .forEach(
                              i -> {
                                context.spawnAnonymous(childInitial, message.props);
                              });
                      return Behaviors.same();
                    })
                .onMessage(
                    CreateMessageAdapter.class,
                    message -> {
                      context.messageAdapter(message.clazz, message.f);
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnWatchAndUnWatch.class,
                    message -> {
                      ActorRef<Action> c = context.spawn(childInitial, message.name);
                      context.watch(c);
                      context.unwatch(c);
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnAndWatchWith.class,
                    message -> {
                      ActorRef<Action> c = context.spawn(childInitial, message.name);
                      context.watchWith(c, message);
                      return Behaviors.same();
                    })
                .onMessage(
                    SpawnSession.class,
                    message -> {
                      ActorRef<String> session =
                          context.spawnAnonymous(
                              Behaviors.receiveMessage(
                                  m -> {
                                    message.sessionHandler.tell(m);
                                    return Behaviors.same();
                                  }));
                      message.replyTo.tell(session);
                      return Behaviors.same();
                    })
                .onMessage(
                    KillSession.class,
                    message -> {
                      context.stop(message.session);
                      message.replyTo.tell(Done.getInstance());
                      return Behaviors.same();
                    })
                .onMessage(
                    Log.class,
                    message -> {
                      context.getLog().info(message.what);
                      return Behaviors.same();
                    })
                .onMessage(
                    AskForCookiesFrom.class,
                    message -> {
                      context.ask(
                          CookiesForYou.class,
                          message.distributor,
                          Duration.ofSeconds(10),
                          (ActorRef<CookiesForYou> ref) -> new GiveMeCookies(6, ref),
                          (response, throwable) -> {
                            if (response != null) {
                              return new Log(
                                  "Got " + response.nrCookies + " cookies from distributor");
                            } else {
                              return new Log("Failed to get cookies: " + throwable.getMessage());
                            }
                          });
                      return Behaviors.same();
                    })
                .build();
          });

  @Test
  public void allowAssertionsOnEffectType() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnChildren(1));
    Effect.Spawned spawned = test.expectEffectClass(Effect.Spawned.class);
    assertEquals(spawned.childName(), "child0");
  }

  @Test
  public void allowExpectingNoEffects() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.expectEffect(Effects.noEffects());
  }

  @Test
  public void allowsExpectingNoEffectByType() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.expectEffectClass(Effect.NoEffects.class);
  }

  @Test
  public void allowRetrieveAllLogs() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    String what = "Hello!";
    test.run(new Log(what));
    final List<CapturedLogEvent> allLogEntries = test.getAllLogEntries();
    assertEquals(1, allLogEntries.size());
    assertEquals(new CapturedLogEvent(Level.INFO, what), allLogEntries.get(0));
  }

  @Test
  public void allowClearLogs() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    String what = "Hello!";
    test.run(new Log(what));
    assertEquals(1, test.getAllLogEntries().size());
    test.clearLog();
    assertEquals(0, test.getAllLogEntries().size());
  }

  @Test
  public void returnEffectsThatHaveTakenPlace() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    assertFalse(test.hasEffects());
    test.run(new SpawnChildrenAnonymous(1));
    assertTrue(test.hasEffects());
  }

  @Test
  @Ignore("Not supported for Java API")
  public void allowAssertionsUsingPartialFunctions() {}

  @Test
  public void spawnChildrenWithNoProps() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnChildren(2));
    List<Effect> allEffects = test.getAllEffects();
    assertEquals(
        Arrays.asList(
            Effects.spawned(childInitial, "child0"),
            Effects.spawned(childInitial, "child1", Props.empty())),
        allEffects);
  }

  @Test
  public void spawnChildrenWithProps() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnChildrenWithProps(1, props));
    assertEquals(props, test.expectEffectClass(Effect.Spawned.class).props());
  }

  @Test
  public void spawnAnonChildrenWithNoProps() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnChildrenAnonymous(2));
    List<Effect> allEffects = test.getAllEffects();
    assertEquals(
        Arrays.asList(
            Effects.spawnedAnonymous(childInitial),
            Effects.spawnedAnonymous(childInitial, Props.empty())),
        allEffects);
  }

  @Test
  public void spawnAnonChildrenWithProps() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnChildrenAnonymousWithProps(1, props));
    assertEquals(props, test.expectEffectClass(Effect.SpawnedAnonymous.class).props());
  }

  @Test
  public void createMessageAdapters() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    SpawnChildren adaptedMessage = new SpawnChildren(1);
    test.run(new CreateMessageAdapter(String.class, o -> adaptedMessage));
    @SuppressWarnings("unchecked")
    Effect.MessageAdapter<String, SpawnChildren> mAdapter =
        test.<Effect.MessageAdapter>expectEffectClass(Effect.MessageAdapter.class);
    assertEquals(String.class, mAdapter.messageClass());
    assertEquals(adaptedMessage, mAdapter.adaptFunction().apply("anything"));
  }

  @Test
  public void recordWatching() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    test.run(new SpawnWatchAndUnWatch("name"));
    ActorRef<Object> child = test.childInbox("name").getRef();
    test.expectEffectClass(Effect.Spawned.class);
    assertEquals(child, test.expectEffectClass(Effect.Watched.class).other());
    assertEquals(child, test.expectEffectClass(Effect.Unwatched.class).other());
  }

  @Test
  public void recordWatchWith() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    SpawnAndWatchWith spawnAndWatchWithMsg = new SpawnAndWatchWith("name");
    test.run(spawnAndWatchWithMsg);
    ActorRef<Object> child = test.childInbox("name").getRef();
    test.expectEffectClass(Effect.Spawned.class);

    Effect.WatchedWith watchedWith = test.expectEffectClass(Effect.WatchedWith.class);
    assertEquals(child, watchedWith.other());
    assertEquals(spawnAndWatchWithMsg, watchedWith.message());
  }

  @Test
  public void allowRetrievingAndKilling() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    TestInbox<String> h = TestInbox.create();

    ReplyInbox<ActorRef<String>> sessionReply =
        test.runAsk(replyTo -> new SpawnSession(replyTo, h.getRef()));

    ActorRef<String> sessionRef = sessionReply.receiveReply();

    Effect.SpawnedAnonymous s = test.expectEffectClass(Effect.SpawnedAnonymous.class);
    assertEquals(sessionRef, s.ref());

    BehaviorTestKit<String> session = test.childTestKit(sessionRef);
    session.run("hello");
    assertEquals(Collections.singletonList("hello"), h.getAllReceived());

    ReplyInbox<Done> doneReply = test.runAsk(replyTo -> new KillSession(sessionRef, replyTo));
    doneReply.expectReply(Done.getInstance());

    test.expectEffectClass(Effect.Stopped.class);
  }

  @Test
  public void canUseTimerScheduledInJavaApi() {
    // this is a compilation test
    Effect.TimerScheduled<String> timerScheduled =
        Effects.timerScheduled(
            "my key",
            "my msg",
            Duration.ofSeconds(42),
            Effect.timerScheduled().fixedDelayMode(),
            false,
            () -> {});
    assertNotNull(timerScheduled);
  }

  @Test
  public void reifyAskAsEffect() {
    BehaviorTestKit<Command> test = BehaviorTestKit.create(behavior);
    TestInbox<CookieDistributorCommand> cdInbox = TestInbox.create();

    test.run(new AskForCookiesFrom(cdInbox.getRef()));

    Effect expectedEffect =
        Effects.askInitiated(
            cdInbox.getRef(), Duration.ofSeconds(10), CookiesForYou.class, Command.class);
    Effect.AskInitiated actualEffect = test.expectEffectClass(Effect.AskInitiated.class);

    assertEquals(actualEffect, expectedEffect);

    // Other functionality is tested in the scaladsl
  }
}
