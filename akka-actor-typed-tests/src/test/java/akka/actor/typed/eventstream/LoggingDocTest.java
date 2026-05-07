/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.eventstream;

// #imports
import akka.actor.AllDeadLetters;
import akka.actor.SuppressedDeadLetter;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.actor.typed.SpawnProtocol.Spawn;
import akka.actor.typed.eventstream.EventStream.Publish;
import akka.actor.typed.eventstream.EventStream.Subscribe;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
// #imports-deadletter
import akka.actor.DeadLetter;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
// #imports-deadletter

public class LoggingDocTest extends JUnitSuite {

    @Test
    public void subscribeToDeadLetters() {
        // #deadletters
        ActorSystem<DeadLetter> system = ActorSystem.create(Behaviors.empty(), "DeadLetters");
        system.eventStream().tell(new Subscribe<>(DeadLetter.class, system));
        // #deadletters
        ActorTestKit.shutdown(system);
    }

    public
    // #deadletter-actor
    static class DeadLetterActor extends AbstractBehavior<String> {

        public static Behavior<String> create() {
            return Behaviors.setup(DeadLetterActor::new);
        }

        public DeadLetterActor(ActorContext<String> context) {
            super(context);
            ActorRef<DeadLetter> messageAdapter = context.messageAdapter(
                DeadLetter.class,
                d -> d.message().toString()
            );
            context.getSystem().eventStream()
                .tell(new Subscribe<>(DeadLetter.class, messageAdapter));
        }

        @Override
        public Receive<String> createReceive() {
            return newReceiveBuilder().onMessage(String.class, msg -> {
                System.out.println(msg);
                return Behaviors.same();
            }).build();
        }
    }
    // #deadletter-actor

    // #superclass-subscription-eventstream
    interface AllKindsOfMusic {

    }

    class Jazz implements AllKindsOfMusic {

        public final String artist;

        public Jazz(String artist) {
            this.artist = artist;
        }
    }

    class Electronic implements AllKindsOfMusic {

        public final String artist;

        public Electronic(String artist) {
            this.artist = artist;
        }
    }

    static class Listener extends AbstractBehavior<AllKindsOfMusic> {

        public static Behavior<AllKindsOfMusic> create() {
            return Behaviors.setup(Listener::new);
        }

        public Listener(ActorContext<AllKindsOfMusic> context) {
            super(context);
        }


        @Override
        public Receive<AllKindsOfMusic> createReceive() {
            return newReceiveBuilder()
                .onMessage(Jazz.class, msg -> {
                    System.out.printf("%s is listening to: %s%n",
                        getContext().getSelf().path().name(),
                        msg);
                    return Behaviors.same();
                })
                .onMessage(Electronic.class, msg -> {
                    System.out.printf("%s is listening to: %s%n",
                        getContext().getSelf().path().name(),
                        msg);
                    return Behaviors.same();
                }).build();
        }
    }
    // #superclass-subscription-eventstream

    @Test
    public void subscribeBySubclassification() {
        // #superclass-subscription-eventstream
        ActorSystem<SpawnProtocol.Command> system = ActorSystem.create(SpawnProtocol.create(),
            "Subclassification");
        final Duration timeout = Duration.ofSeconds(3);

        CompletionStage<ActorRef<Jazz>> jazzListener = AskPattern.ask(
            system,
            replyTo -> new Spawn<>(Listener.create().narrow(), "jazzListener", Props.empty(), replyTo),
            timeout,
            system.scheduler()
        );

        CompletionStage<ActorRef<AllKindsOfMusic>> musicListener = AskPattern.ask(
            system,
            replyTo -> new Spawn<>(Listener.create(), "musicListener", Props.empty(), replyTo),
            timeout,
            system.scheduler()
        );

        ActorRef<Jazz> jazzListenerActorRef = jazzListener.toCompletableFuture().join();
        ActorRef<AllKindsOfMusic> musicListenerActorRef = musicListener.toCompletableFuture()
            .join();

        system.eventStream().tell(new Subscribe<>(Jazz.class, jazzListenerActorRef));
        system.eventStream().tell(new Subscribe<>(AllKindsOfMusic.class, musicListenerActorRef));
        // only musicListener gets this message, since it listens to *all* kinds of music:
        system.eventStream().tell(new Publish<>(new Electronic("Parov Stelar")));

        // jazzListener and musicListener will be notified about Jazz:
        system.eventStream().tell(new Publish<>(new Jazz("Sonny Rollins")));

        // #superclass-subscription-eventstream
        ActorTestKit.shutdown(system);
    }

    @Test
    public void subscribeToSuppressedDeadLetters() {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "SuppressedDeadLetter");
        TestProbe<SuppressedDeadLetter> probe = TestProbe.create(system);
        ActorRef<SuppressedDeadLetter> actor = probe.ref();
        // #suppressed-deadletters
        system.eventStream().tell(new Subscribe<>(SuppressedDeadLetter.class, actor));
        // #suppressed-deadletters

        ActorTestKit.shutdown(system);
    }

    @Test
    public void subscribeToAllDeadLetters() {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "AllDeadLetters");
        TestProbe<AllDeadLetters> probe = TestProbe.create(system);
        ActorRef<AllDeadLetters> actor = probe.ref();
        // #all-deadletters
        system.eventStream().tell(new Subscribe<>(AllDeadLetters.class, actor));
        // #all-deadletters

        ActorTestKit.shutdown(system);
    }
}
