/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import static jdocs.akka.typed.InteractionPatternsTest.Samples.Buncher;
import static jdocs.akka.typed.InteractionPatternsTest.Samples.Printer;
import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class InteractionPatternsTest extends JUnitSuite {

  interface Samples {

    // #fire-and-forget-definition
    public class Printer {
      public static class PrintMe {
        public final String message;

        public PrintMe(String message) {
          this.message = message;
        }
      }

      public static Behavior<PrintMe> create() {
        return Behaviors.setup(
            context ->
                Behaviors.receive(PrintMe.class)
                    .onMessage(
                        PrintMe.class,
                        printMe -> {
                          context.getLog().info(printMe.message);
                          return Behaviors.same();
                        })
                    .build());
      }
    }
    // #fire-and-forget-definition

    public class CookieFabric {
      // #request-response-protocol
      public static class Request {
        public final String query;
        public final ActorRef<Response> replyTo;

        public Request(String query, ActorRef<Response> replyTo) {
          this.query = query;
          this.replyTo = replyTo;
        }
      }

      public static class Response {
        public final String result;

        public Response(String result) {
          this.result = result;
        }
      }
      // #request-response-protocol

      // #request-response-respond
      // actor behavior
      public static Behavior<Request> create() {
        return Behaviors.receive(Request.class)
            .onMessage(Request.class, CookieFabric::onRequest)
            .build();
      }

      private static Behavior<Request> onRequest(Request request) {
        // ... process request ...
        request.replyTo.tell(new Response("Here are the cookies for " + request.query));
        return Behaviors.same();
      }
      // #request-response-respond

      void demo() {
        ActorRef<CookieFabric.Request> cookieFabric = null;
        ActorContext<CookieFabric.Response> context = null;

        // #request-response-send
        cookieFabric.tell(new CookieFabric.Request("give me cookies", context.getSelf()));
        // #request-response-send

        // #ignore-reply
        cookieFabric.tell(
            new CookieFabric.Request("don't send cookies back", context.getSystem().ignoreRef()));
        // #ignore-reply
      }
    }

    // #adapted-response

    public class Backend {
      public interface Request {}

      public static class StartTranslationJob implements Request {
        public final int taskId;
        public final URI site;
        public final ActorRef<Response> replyTo;

        public StartTranslationJob(int taskId, URI site, ActorRef<Response> replyTo) {
          this.taskId = taskId;
          this.site = site;
          this.replyTo = replyTo;
        }
      }

      public interface Response {}

      public static class JobStarted implements Response {
        public final int taskId;

        public JobStarted(int taskId) {
          this.taskId = taskId;
        }
      }

      public static class JobProgress implements Response {
        public final int taskId;
        public final double progress;

        public JobProgress(int taskId, double progress) {
          this.taskId = taskId;
          this.progress = progress;
        }
      }

      public static class JobCompleted implements Response {
        public final int taskId;
        public final URI result;

        public JobCompleted(int taskId, URI result) {
          this.taskId = taskId;
          this.result = result;
        }
      }
    }

    public class Frontend {

      public interface Command {}

      public static class Translate implements Command {
        public final URI site;
        public final ActorRef<URI> replyTo;

        public Translate(URI site, ActorRef<URI> replyTo) {
          this.site = site;
          this.replyTo = replyTo;
        }
      }

      private static class WrappedBackendResponse implements Command {
        final Backend.Response response;

        public WrappedBackendResponse(Backend.Response response) {
          this.response = response;
        }
      }

      public static class Translator extends AbstractBehavior<Command> {
        private final ActorRef<Backend.Request> backend;
        private final ActorRef<Backend.Response> backendResponseAdapter;

        private int taskIdCounter = 0;
        private Map<Integer, ActorRef<URI>> inProgress = new HashMap<>();

        public Translator(ActorContext<Command> context, ActorRef<Backend.Request> backend) {
          super(context);
          this.backend = backend;
          this.backendResponseAdapter =
              context.messageAdapter(Backend.Response.class, WrappedBackendResponse::new);
        }

        @Override
        public Receive<Command> createReceive() {
          return newReceiveBuilder()
              .onMessage(Translate.class, this::onTranslate)
              .onMessage(WrappedBackendResponse.class, this::onWrappedBackendResponse)
              .build();
        }

        private Behavior<Command> onTranslate(Translate cmd) {
          taskIdCounter += 1;
          inProgress.put(taskIdCounter, cmd.replyTo);
          backend.tell(
              new Backend.StartTranslationJob(taskIdCounter, cmd.site, backendResponseAdapter));
          return this;
        }

        private Behavior<Command> onWrappedBackendResponse(WrappedBackendResponse wrapped) {
          Backend.Response response = wrapped.response;
          if (response instanceof Backend.JobStarted) {
            Backend.JobStarted rsp = (Backend.JobStarted) response;
            getContext().getLog().info("Started {}", rsp.taskId);
          } else if (response instanceof Backend.JobProgress) {
            Backend.JobProgress rsp = (Backend.JobProgress) response;
            getContext().getLog().info("Progress {}", rsp.taskId);
          } else if (response instanceof Backend.JobCompleted) {
            Backend.JobCompleted rsp = (Backend.JobCompleted) response;
            getContext().getLog().info("Completed {}", rsp.taskId);
            inProgress.get(rsp.taskId).tell(rsp.result);
            inProgress.remove(rsp.taskId);
          } else {
            return Behaviors.unhandled();
          }

          return this;
        }
      }
    }
    // #adapted-response

    // #timer
    public class Buncher {

      public interface Command {}

      public static final class Batch {
        private final List<Command> messages;

        public Batch(List<Command> messages) {
          this.messages = Collections.unmodifiableList(messages);
        }

        public List<Command> getMessages() {
          return messages;
        }
        // #timer
        @Override
        public boolean equals(Object o) {
          if (this == o) return true;
          if (o == null || getClass() != o.getClass()) return false;
          Batch batch = (Batch) o;
          return Objects.equals(messages, batch.messages);
        }

        @Override
        public int hashCode() {
          return Objects.hash(messages);
        } // #timer
      }

      public static final class ExcitingMessage implements Command {
        public final String message;

        public ExcitingMessage(String message) {
          this.message = message;
        }
      }

      private static final Object TIMER_KEY = new Object();

      private enum Timeout implements Command {
        INSTANCE
      }

      public static Behavior<Command> create(ActorRef<Batch> target, Duration after, int maxSize) {
        return Behaviors.withTimers(timers -> new Buncher(timers, target, after, maxSize).idle());
      }

      private final TimerScheduler<Command> timers;
      private final ActorRef<Batch> target;
      private final Duration after;
      private final int maxSize;

      private Buncher(
          TimerScheduler<Command> timers, ActorRef<Batch> target, Duration after, int maxSize) {
        this.timers = timers;
        this.target = target;
        this.after = after;
        this.maxSize = maxSize;
      }

      private Behavior<Command> idle() {
        return Behaviors.receive(Command.class)
            .onMessage(Command.class, this::onIdleCommand)
            .build();
      }

      private Behavior<Command> onIdleCommand(Command message) {
        timers.startSingleTimer(TIMER_KEY, Timeout.INSTANCE, after);
        return Behaviors.setup(context -> new Active(context, message));
      }

      private class Active extends AbstractBehavior<Command> {

        private final List<Command> buffer = new ArrayList<>();

        Active(ActorContext<Command> context, Command firstCommand) {
          super(context);
          buffer.add(firstCommand);
        }

        @Override
        public Receive<Command> createReceive() {
          return newReceiveBuilder()
              .onMessage(Timeout.class, message -> onTimeout())
              .onMessage(Command.class, this::onCommand)
              .build();
        }

        private Behavior<Command> onTimeout() {
          target.tell(new Batch(buffer));
          return idle(); // switch to idle
        }

        private Behavior<Command> onCommand(Command message) {
          buffer.add(message);
          if (buffer.size() == maxSize) {
            timers.cancel(TIMER_KEY);
            target.tell(new Batch(buffer));
            return idle(); // switch to idle
          } else {
            return this; // stay Active
          }
        }
      }
    }
    // #timer

    // #actor-ask
    public class Hal extends AbstractBehavior<Hal.Command> {

      public static Behavior<Hal.Command> create() {
        return Behaviors.setup(Hal::new);
      }

      private Hal(ActorContext<Command> context) {
        super(context);
      }

      public interface Command {}

      public static final class OpenThePodBayDoorsPlease implements Command {
        public final ActorRef<HalResponse> respondTo;

        public OpenThePodBayDoorsPlease(ActorRef<HalResponse> respondTo) {
          this.respondTo = respondTo;
        }
      }

      public static final class HalResponse {
        public final String message;

        public HalResponse(String message) {
          this.message = message;
        }
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(OpenThePodBayDoorsPlease.class, this::onOpenThePodBayDoorsPlease)
            .build();
      }

      private Behavior<Command> onOpenThePodBayDoorsPlease(OpenThePodBayDoorsPlease message) {
        message.respondTo.tell(new HalResponse("I'm sorry, Dave. I'm afraid I can't do that."));
        return this;
      }
    }

    public class Dave extends AbstractBehavior<Dave.Command> {

      public interface Command {}

      // this is a part of the protocol that is internal to the actor itself
      private static final class AdaptedResponse implements Command {
        public final String message;

        public AdaptedResponse(String message) {
          this.message = message;
        }
      }

      public static Behavior<Command> create(ActorRef<Hal.Command> hal) {
        return Behaviors.setup(context -> new Dave(context, hal));
      }

      private Dave(ActorContext<Command> context, ActorRef<Hal.Command> hal) {
        super(context);

        // asking someone requires a timeout, if the timeout hits without response
        // the ask is failed with a TimeoutException
        final Duration timeout = Duration.ofSeconds(3);

        context.ask(
            Hal.HalResponse.class,
            hal,
            timeout,
            // construct the outgoing message
            (ActorRef<Hal.HalResponse> ref) -> new Hal.OpenThePodBayDoorsPlease(ref),
            // adapt the response (or failure to respond)
            (response, throwable) -> {
              if (response != null) {
                return new AdaptedResponse(response.message);
              } else {
                return new AdaptedResponse("Request failed");
              }
            });

        // we can also tie in request context into an interaction, it is safe to look at
        // actor internal state from the transformation function, but remember that it may have
        // changed at the time the response arrives and the transformation is done, best is to
        // use immutable state we have closed over like here.
        final int requestId = 1;
        context.ask(
            Hal.HalResponse.class,
            hal,
            timeout,
            // construct the outgoing message
            (ActorRef<Hal.HalResponse> ref) -> new Hal.OpenThePodBayDoorsPlease(ref),
            // adapt the response (or failure to respond)
            (response, throwable) -> {
              if (response != null) {
                return new AdaptedResponse(requestId + ": " + response.message);
              } else {
                return new AdaptedResponse(requestId + ": Request failed");
              }
            });
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            // the adapted message ends up being processed like any other
            // message sent to the actor
            .onMessage(AdaptedResponse.class, this::onAdaptedResponse)
            .build();
      }

      private Behavior<Command> onAdaptedResponse(AdaptedResponse response) {
        getContext().getLog().info("Got response from HAL: {}", response.message);
        return this;
      }
    }
    // #actor-ask

    // #per-session-child
    // dummy data types just for this sample
    public class Keys {}

    public class Wallet {}

    public class KeyCabinet {
      public static class GetKeys {
        public final String whoseKeys;
        public final ActorRef<Keys> replyTo;

        public GetKeys(String whoseKeys, ActorRef<Keys> respondTo) {
          this.whoseKeys = whoseKeys;
          this.replyTo = respondTo;
        }
      }

      public static Behavior<GetKeys> create() {
        return Behaviors.receiveMessage(KeyCabinet::onGetKeys);
      }

      private static Behavior<GetKeys> onGetKeys(GetKeys message) {
        message.replyTo.tell(new Keys());
        return Behaviors.same();
      }
    }

    public class Drawer {

      public static class GetWallet {
        public final String whoseWallet;
        public final ActorRef<Wallet> replyTo;

        public GetWallet(String whoseWallet, ActorRef<Wallet> replyTo) {
          this.whoseWallet = whoseWallet;
          this.replyTo = replyTo;
        }
      }

      public static Behavior<GetWallet> create() {
        return Behaviors.receiveMessage(Drawer::onGetWallet);
      }

      private static Behavior<GetWallet> onGetWallet(GetWallet message) {
        message.replyTo.tell(new Wallet());
        return Behaviors.same();
      }
    }

    public class Home {

      public interface Command {}

      public static class LeaveHome implements Command {
        public final String who;
        public final ActorRef<ReadyToLeaveHome> respondTo;

        public LeaveHome(String who, ActorRef<ReadyToLeaveHome> respondTo) {
          this.who = who;
          this.respondTo = respondTo;
        }
      }

      public static class ReadyToLeaveHome {
        public final String who;
        public final Keys keys;
        public final Wallet wallet;

        public ReadyToLeaveHome(String who, Keys keys, Wallet wallet) {
          this.who = who;
          this.keys = keys;
          this.wallet = wallet;
        }
      }

      private final ActorContext<Command> context;

      private final ActorRef<KeyCabinet.GetKeys> keyCabinet;
      private final ActorRef<Drawer.GetWallet> drawer;

      private Home(ActorContext<Command> context) {
        this.context = context;
        this.keyCabinet = context.spawn(KeyCabinet.create(), "key-cabinet");
        this.drawer = context.spawn(Drawer.create(), "drawer");
      }

      private Behavior<Command> behavior() {
        return Behaviors.receive(Command.class)
            .onMessage(LeaveHome.class, this::onLeaveHome)
            .build();
      }

      private Behavior<Command> onLeaveHome(LeaveHome message) {
        context.spawn(
            PrepareToLeaveHome.create(message.who, message.respondTo, keyCabinet, drawer),
            "leaving" + message.who);
        return Behaviors.same();
      }

      // actor behavior
      public static Behavior<Command> create() {
        return Behaviors.setup(context -> new Home(context).behavior());
      }
    }

    // per session actor behavior
    class PrepareToLeaveHome extends AbstractBehavior<Object> {
      static Behavior<Object> create(
          String whoIsLeaving,
          ActorRef<Home.ReadyToLeaveHome> replyTo,
          ActorRef<KeyCabinet.GetKeys> keyCabinet,
          ActorRef<Drawer.GetWallet> drawer) {
        return Behaviors.setup(
            context -> new PrepareToLeaveHome(context, whoIsLeaving, replyTo, keyCabinet, drawer));
      }

      private final String whoIsLeaving;
      private final ActorRef<Home.ReadyToLeaveHome> replyTo;
      private final ActorRef<KeyCabinet.GetKeys> keyCabinet;
      private final ActorRef<Drawer.GetWallet> drawer;
      private Optional<Wallet> wallet = Optional.empty();
      private Optional<Keys> keys = Optional.empty();

      private PrepareToLeaveHome(
          ActorContext<Object> context,
          String whoIsLeaving,
          ActorRef<Home.ReadyToLeaveHome> replyTo,
          ActorRef<KeyCabinet.GetKeys> keyCabinet,
          ActorRef<Drawer.GetWallet> drawer) {
        super(context);
        this.whoIsLeaving = whoIsLeaving;
        this.replyTo = replyTo;
        this.keyCabinet = keyCabinet;
        this.drawer = drawer;
      }

      @Override
      public Receive<Object> createReceive() {
        return newReceiveBuilder()
            .onMessage(Wallet.class, this::onWallet)
            .onMessage(Keys.class, this::onKeys)
            .build();
      }

      private Behavior<Object> onWallet(Wallet wallet) {
        this.wallet = Optional.of(wallet);
        return completeOrContinue();
      }

      private Behavior<Object> onKeys(Keys keys) {
        this.keys = Optional.of(keys);
        return completeOrContinue();
      }

      private Behavior<Object> completeOrContinue() {
        if (wallet.isPresent() && keys.isPresent()) {
          replyTo.tell(new Home.ReadyToLeaveHome(whoIsLeaving, keys.get(), wallet.get()));
          return Behaviors.stopped();
        } else {
          return this;
        }
      }
    }
    // #per-session-child

  }

  interface StandaloneAskSample {
    // #standalone-ask
    public class CookieFabric extends AbstractBehavior<CookieFabric.Command> {

      interface Command {}

      public static class GiveMeCookies implements Command {
        public final int count;
        public final ActorRef<Reply> replyTo;

        public GiveMeCookies(int count, ActorRef<Reply> replyTo) {
          this.count = count;
          this.replyTo = replyTo;
        }
      }

      interface Reply {}

      public static class Cookies implements Reply {
        public final int count;

        public Cookies(int count) {
          this.count = count;
        }
      }

      public static class InvalidRequest implements Reply {
        public final String reason;

        public InvalidRequest(String reason) {
          this.reason = reason;
        }
      }

      public static Behavior<Command> create() {
        return Behaviors.setup(CookieFabric::new);
      }

      private CookieFabric(ActorContext<Command> context) {
        super(context);
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder().onMessage(GiveMeCookies.class, this::onGiveMeCookies).build();
      }

      private Behavior<Command> onGiveMeCookies(GiveMeCookies request) {
        if (request.count >= 5) request.replyTo.tell(new InvalidRequest("Too many cookies."));
        else request.replyTo.tell(new Cookies(request.count));

        return this;
      }
    }
    // #standalone-ask

    class NotShown {

      // #standalone-ask

      public void askAndPrint(
          ActorSystem<Void> system, ActorRef<CookieFabric.Command> cookieFabric) {
        CompletionStage<CookieFabric.Reply> result =
            AskPattern.ask(
                cookieFabric,
                replyTo -> new CookieFabric.GiveMeCookies(3, replyTo),
                // asking someone requires a timeout and a scheduler, if the timeout hits without
                // response the ask is failed with a TimeoutException
                Duration.ofSeconds(3),
                system.scheduler());

        result.whenComplete(
            (reply, failure) -> {
              if (reply instanceof CookieFabric.Cookies)
                System.out.println("Yay, " + ((CookieFabric.Cookies) reply).count + " cookies!");
              else if (reply instanceof CookieFabric.InvalidRequest)
                System.out.println(
                    "No cookies for me. " + ((CookieFabric.InvalidRequest) reply).reason);
              else System.out.println("Boo! didn't get cookies in time. " + failure);
            });
      }
      // #standalone-ask

      public void askAndMapInvalid(
          ActorSystem<Void> system, ActorRef<CookieFabric.Command> cookieFabric) {
        // #standalone-ask-fail-future
        CompletionStage<CookieFabric.Reply> result =
            AskPattern.ask(
                cookieFabric,
                replyTo -> new CookieFabric.GiveMeCookies(3, replyTo),
                Duration.ofSeconds(3),
                system.scheduler());

        CompletionStage<CookieFabric.Cookies> cookies =
            result.thenCompose(
                (CookieFabric.Reply reply) -> {
                  if (reply instanceof CookieFabric.Cookies) {
                    return CompletableFuture.completedFuture((CookieFabric.Cookies) reply);
                  } else if (reply instanceof CookieFabric.InvalidRequest) {
                    CompletableFuture<CookieFabric.Cookies> failed = new CompletableFuture<>();
                    failed.completeExceptionally(
                        new IllegalArgumentException(((CookieFabric.InvalidRequest) reply).reason));
                    return failed;
                  } else {
                    throw new IllegalStateException("Unexpected reply: " + reply.getClass());
                  }
                });

        cookies.whenComplete(
            (cookiesReply, failure) -> {
              if (cookiesReply != null)
                System.out.println("Yay, " + cookiesReply.count + " cookies!");
              else System.out.println("Boo! didn't get cookies in time. " + failure);
            });
        // #standalone-ask-fail-future
      }
    }
  }

  interface PipeToSelfSample {
    // #pipeToSelf
    public interface CustomerDataAccess {
      CompletionStage<Done> update(Customer customer);
    }

    public class Customer {
      public final String id;
      public final long version;
      public final String name;
      public final String address;

      public Customer(String id, long version, String name, String address) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.address = address;
      }
    }

    public class CustomerRepository extends AbstractBehavior<CustomerRepository.Command> {

      private static final int MAX_OPERATIONS_IN_PROGRESS = 10;

      interface Command {}

      public static class Update implements Command {
        public final Customer customer;
        public final ActorRef<OperationResult> replyTo;

        public Update(Customer customer, ActorRef<OperationResult> replyTo) {
          this.customer = customer;
          this.replyTo = replyTo;
        }
      }

      interface OperationResult {}

      public static class UpdateSuccess implements OperationResult {
        public final String id;

        public UpdateSuccess(String id) {
          this.id = id;
        }
      }

      public static class UpdateFailure implements OperationResult {
        public final String id;
        public final String reason;

        public UpdateFailure(String id, String reason) {
          this.id = id;
          this.reason = reason;
        }
      }

      private static class WrappedUpdateResult implements Command {
        public final OperationResult result;
        public final ActorRef<OperationResult> replyTo;

        private WrappedUpdateResult(OperationResult result, ActorRef<OperationResult> replyTo) {
          this.result = result;
          this.replyTo = replyTo;
        }
      }

      public static Behavior<Command> create(CustomerDataAccess dataAccess) {
        return Behaviors.setup(context -> new CustomerRepository(context, dataAccess));
      }

      private final CustomerDataAccess dataAccess;
      private int operationsInProgress = 0;

      private CustomerRepository(ActorContext<Command> context, CustomerDataAccess dataAccess) {
        super(context);
        this.dataAccess = dataAccess;
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(Update.class, this::onUpdate)
            .onMessage(WrappedUpdateResult.class, this::onUpdateResult)
            .build();
      }

      private Behavior<Command> onUpdate(Update command) {
        if (operationsInProgress == MAX_OPERATIONS_IN_PROGRESS) {
          command.replyTo.tell(
              new UpdateFailure(
                  command.customer.id,
                  "Max " + MAX_OPERATIONS_IN_PROGRESS + " concurrent operations supported"));
        } else {
          // increase operationsInProgress counter
          operationsInProgress++;
          CompletionStage<Done> futureResult = dataAccess.update(command.customer);
          getContext()
              .pipeToSelf(
                  futureResult,
                  (ok, exc) -> {
                    if (exc == null)
                      return new WrappedUpdateResult(
                          new UpdateSuccess(command.customer.id), command.replyTo);
                    else
                      return new WrappedUpdateResult(
                          new UpdateFailure(command.customer.id, exc.getMessage()),
                          command.replyTo);
                  });
        }
        return this;
      }

      private Behavior<Command> onUpdateResult(WrappedUpdateResult wrapped) {
        // decrease operationsInProgress counter
        operationsInProgress--;
        // send result to original requestor
        wrapped.replyTo.tell(wrapped.result);
        return this;
      }
    }
    // #pipeToSelf

  }

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void fireAndForgetSample() throws Exception {
    // #fire-and-forget-doit
    final ActorSystem<Printer.PrintMe> system =
        ActorSystem.create(Printer.create(), "printer-sample-system");

    // note that system is also the ActorRef to the guardian actor
    final ActorRef<Printer.PrintMe> ref = system;

    // these are all fire and forget
    ref.tell(new Printer.PrintMe("message 1"));
    ref.tell(new Printer.PrintMe("message 2"));
    // #fire-and-forget-doit

    system.terminate();
    system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @Test
  public void timers() throws Exception {
    TestProbe<Buncher.Batch> probe = testKit.createTestProbe(Buncher.Batch.class);
    ActorRef<Buncher.Command> buncher =
        testKit.spawn(Buncher.create(probe.ref(), Duration.ofSeconds(1), 10), "batcher");

    Buncher.ExcitingMessage msgOne = new Buncher.ExcitingMessage("one");
    Buncher.ExcitingMessage msgTwo = new Buncher.ExcitingMessage("two");
    buncher.tell(msgOne);
    buncher.tell(msgTwo);
    probe.expectNoMessage();
    probe.expectMessage(Duration.ofSeconds(2), new Buncher.Batch(Arrays.asList(msgOne, msgTwo)));
  }

  @Test
  public void testPipeToSelf() {

    PipeToSelfSample.CustomerDataAccess dataAccess =
        new PipeToSelfSample.CustomerDataAccess() {
          @Override
          public CompletionStage<Done> update(PipeToSelfSample.Customer customer) {
            return CompletableFuture.completedFuture(Done.getInstance());
          }
        };

    ActorRef<PipeToSelfSample.CustomerRepository.Command> repository =
        testKit.spawn(PipeToSelfSample.CustomerRepository.create(dataAccess));
    TestProbe<PipeToSelfSample.CustomerRepository.OperationResult> probe =
        testKit.createTestProbe(PipeToSelfSample.CustomerRepository.OperationResult.class);

    repository.tell(
        new PipeToSelfSample.CustomerRepository.Update(
            new PipeToSelfSample.Customer("123", 1L, "Alice", "Fairy tail road 7"),
            probe.getRef()));
    assertEquals(
        "123",
        probe.expectMessageClass(PipeToSelfSample.CustomerRepository.UpdateSuccess.class).id);
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
