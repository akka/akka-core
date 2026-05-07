/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

// #actor-ask-with-status
import akka.pattern.StatusReply;

// #actor-ask-with-status
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class InteractionPatternsAskWithStatusTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  // separate from InteractionPatternsTest to avoid name clashes while keeping the ask samples
  // almost identical
  interface Samples {
    // #actor-ask-with-status
    public class Hal extends AbstractBehavior<Hal.Command> {

      public static Behavior<Hal.Command> create() {
        return Behaviors.setup(Hal::new);
      }

      private Hal(ActorContext<Hal.Command> context) {
        super(context);
      }

      public interface Command {}

      public static final class OpenThePodBayDoorsPlease implements Hal.Command {
        public final ActorRef<StatusReply<String>> respondTo;

        public OpenThePodBayDoorsPlease(ActorRef<StatusReply<String>> respondTo) {
          this.respondTo = respondTo;
        }
      }

      @Override
      public Receive<Hal.Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(Hal.OpenThePodBayDoorsPlease.class, this::onOpenThePodBayDoorsPlease)
            .build();
      }

      private Behavior<Hal.Command> onOpenThePodBayDoorsPlease(
          Hal.OpenThePodBayDoorsPlease message) {
        message.respondTo.tell(StatusReply.error("I'm sorry, Dave. I'm afraid I can't do that."));
        return this;
      }
    }

    public class Dave extends AbstractBehavior<Dave.Command> {

      public interface Command {}

      // this is a part of the protocol that is internal to the actor itself
      private static final class AdaptedResponse implements Dave.Command {
        public final String message;

        public AdaptedResponse(String message) {
          this.message = message;
        }
      }

      public static Behavior<Dave.Command> create(ActorRef<Hal.Command> hal) {
        return Behaviors.setup(context -> new Dave(context, hal));
      }

      private Dave(ActorContext<Dave.Command> context, ActorRef<Hal.Command> hal) {
        super(context);

        // asking someone requires a timeout, if the timeout hits without response
        // the ask is failed with a TimeoutException
        final Duration timeout = Duration.ofSeconds(3);

        context.askWithStatus(
            String.class,
            hal,
            timeout,
            // construct the outgoing message
            (ActorRef<StatusReply<String>> ref) -> new Hal.OpenThePodBayDoorsPlease(ref),
            // adapt the response (or failure to respond)
            (response, throwable) -> {
              if (response != null) {
                // a ReponseWithStatus.success(m) is unwrapped and passed as response
                return new Dave.AdaptedResponse(response);
              } else {
                // a ResponseWithStatus.error will end up as a StatusReply.ErrorMessage()
                // exception here
                return new Dave.AdaptedResponse("Request failed: " + throwable.getMessage());
              }
            });
      }

      @Override
      public Receive<Dave.Command> createReceive() {
        return newReceiveBuilder()
            // the adapted message ends up being processed like any other
            // message sent to the actor
            .onMessage(Dave.AdaptedResponse.class, this::onAdaptedResponse)
            .build();
      }

      private Behavior<Dave.Command> onAdaptedResponse(Dave.AdaptedResponse response) {
        getContext().getLog().info("Got response from HAL: {}", response.message);
        return this;
      }
    }
    // #actor-ask-with-status

  }

  interface StandaloneAskSample {
    // #standalone-ask-with-status
    public class CookieFabric extends AbstractBehavior<CookieFabric.Command> {

      interface Command {}

      public static class GiveMeCookies implements CookieFabric.Command {
        public final int count;
        public final ActorRef<StatusReply<CookieFabric.Cookies>> replyTo;

        public GiveMeCookies(int count, ActorRef<StatusReply<CookieFabric.Cookies>> replyTo) {
          this.count = count;
          this.replyTo = replyTo;
        }
      }

      public static class Cookies {
        public final int count;

        public Cookies(int count) {
          this.count = count;
        }
      }

      public static Behavior<CookieFabric.Command> create() {
        return Behaviors.setup(CookieFabric::new);
      }

      private CookieFabric(ActorContext<CookieFabric.Command> context) {
        super(context);
      }

      @Override
      public Receive<CookieFabric.Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(CookieFabric.GiveMeCookies.class, this::onGiveMeCookies)
            .build();
      }

      private Behavior<CookieFabric.Command> onGiveMeCookies(CookieFabric.GiveMeCookies request) {
        if (request.count >= 5) request.replyTo.tell(StatusReply.error("Too many cookies."));
        else request.replyTo.tell(StatusReply.success(new CookieFabric.Cookies(request.count)));

        return this;
      }
    }
    // #standalone-ask-with-status

    static class NotShown {

      // #standalone-ask-with-status

      public void askAndPrint(
          ActorSystem<Void> system, ActorRef<CookieFabric.Command> cookieFabric) {
        CompletionStage<CookieFabric.Cookies> result =
            AskPattern.askWithStatus(
                cookieFabric,
                replyTo -> new CookieFabric.GiveMeCookies(3, replyTo),
                // asking someone requires a timeout and a scheduler, if the timeout hits without
                // response the ask is failed with a TimeoutException
                Duration.ofSeconds(3),
                system.scheduler());

        result.whenComplete(
            (reply, failure) -> {
              if (reply != null) System.out.println("Yay, " + reply.count + " cookies!");
              else if (failure instanceof StatusReply.ErrorMessage)
                System.out.println("No cookies for me. " + failure.getMessage());
              else System.out.println("Boo! didn't get cookies in time. " + failure);
            });
      }
      // #standalone-ask-with-status

      public void askAndMapInvalid(
          ActorSystem<Void> system, ActorRef<CookieFabric.Command> cookieFabric) {
        // #standalone-ask-with-status-fail-future
        CompletionStage<CookieFabric.Cookies> cookies =
            AskPattern.askWithStatus(
                cookieFabric,
                replyTo -> new CookieFabric.GiveMeCookies(3, replyTo),
                Duration.ofSeconds(3),
                system.scheduler());

        cookies.whenComplete(
            (cookiesReply, failure) -> {
              if (cookiesReply != null)
                System.out.println("Yay, " + cookiesReply.count + " cookies!");
              else System.out.println("Boo! didn't get cookies in time. " + failure);
            });
        // #standalone-ask-with-status-fail-future
      }
    }
  }

  @Test
  public void askWithStatusExample() {
    // no assert but should at least throw if completely broken
    ActorRef<StandaloneAskSample.CookieFabric.Command> cookieFabric =
        testKit.spawn(StandaloneAskSample.CookieFabric.create());
    StandaloneAskSample.NotShown notShown = new StandaloneAskSample.NotShown();
    notShown.askAndPrint(testKit.system(), cookieFabric);
  }

  @Test
  public void askInActorWithStatusExample() {
    // no assert but should at least throw if completely broken
    ActorRef<Samples.Hal.Command> hal = testKit.spawn(Samples.Hal.create());
    ActorRef<Samples.Dave.Command> dave = testKit.spawn(Samples.Dave.create(hal));
  }
}
