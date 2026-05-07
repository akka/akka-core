/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

// #imports
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

// #imports

import akka.actor.typed.Terminated;
import akka.actor.typed.Props;
import akka.actor.typed.DispatcherSelector;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public interface IntroTest {
  public class HelloWorld extends AbstractBehavior<HelloWorld.Greet> {

    public static final class Greet {
      public final String whom;
      public final ActorRef<Greeted> replyTo;

      public Greet(String whom, ActorRef<Greeted> replyTo) {
        this.whom = whom;
        this.replyTo = replyTo;
      }
    }

    public static final class Greeted {
      public final String whom;
      public final ActorRef<Greet> from;

      public Greeted(String whom, ActorRef<Greet> from) {
        this.whom = whom;
        this.from = from;
      }
    }

    public static Behavior<Greet> create() {
      return Behaviors.setup(HelloWorld::new);
    }

    private HelloWorld(ActorContext<Greet> context) {
      super(context);
    }

    @Override
    public Receive<Greet> createReceive() {
      return newReceiveBuilder().onMessage(Greet.class, this::onGreet).build();
    }

    private Behavior<Greet> onGreet(Greet command) {
      getContext().getLog().info("Hello {}!", command.whom);
      command.replyTo.tell(new Greeted(command.whom, getContext().getSelf()));
      return this;
    }
  }

  interface CustomDispatchersExample {

    // #hello-world-main-with-dispatchers
    public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.SayHello> {

      // Start message...
      // #hello-world-main-with-dispatchers
      public static class SayHello {
        public final String name;

        public SayHello(String name) {
          this.name = name;
        }
      }
      // #hello-world-main-with-dispatchers

      public static Behavior<SayHello> create() {
        return Behaviors.setup(HelloWorldMain::new);
      }

      private final ActorRef<HelloWorld.Greet> greeter;

      private HelloWorldMain(ActorContext<SayHello> context) {
        super(context);

        final String dispatcherPath = "akka.actor.default-blocking-io-dispatcher";
        Props greeterProps = DispatcherSelector.fromConfig(dispatcherPath);
        greeter = getContext().spawn(HelloWorld.create(), "greeter", greeterProps);
      }

      // createReceive ...
      // #hello-world-main-with-dispatchers
      @Override
      public Receive<SayHello> createReceive() {
        return null;
      }
      // #hello-world-main-with-dispatchers
    }
    // #hello-world-main-with-dispatchers
  }

  // #chatroom-behavior
  public class ChatRoom {
    // #chatroom-behavior

    // #chatroom-protocol
    static interface RoomCommand {}

    public static final class GetSession implements RoomCommand {
      public final String screenName;
      public final ActorRef<SessionEvent> replyTo;

      public GetSession(String screenName, ActorRef<SessionEvent> replyTo) {
        this.screenName = screenName;
        this.replyTo = replyTo;
      }
    }
    // #chatroom-protocol
    // #chatroom-behavior
    private static final class PublishSessionMessage implements RoomCommand {
      public final String screenName;
      public final String message;

      public PublishSessionMessage(String screenName, String message) {
        this.screenName = screenName;
        this.message = message;
      }
    }
    // #chatroom-behavior
    // #chatroom-protocol

    interface SessionEvent {}

    public static final class SessionGranted implements SessionEvent {
      public final ActorRef<PostMessage> handle;

      public SessionGranted(ActorRef<PostMessage> handle) {
        this.handle = handle;
      }
    }

    public static final class SessionDenied implements SessionEvent {
      public final String reason;

      public SessionDenied(String reason) {
        this.reason = reason;
      }
    }

    public static final class MessagePosted implements SessionEvent {
      public final String screenName;
      public final String message;

      public MessagePosted(String screenName, String message) {
        this.screenName = screenName;
        this.message = message;
      }
    }

    interface SessionCommand {}

    public static final class PostMessage implements SessionCommand {
      public final String message;

      public PostMessage(String message) {
        this.message = message;
      }
    }

    private static final class NotifyClient implements SessionCommand {
      final MessagePosted message;

      NotifyClient(MessagePosted message) {
        this.message = message;
      }
    }
    // #chatroom-protocol
    // #chatroom-behavior

    public static Behavior<RoomCommand> create() {
      return Behaviors.setup(
          ctx -> new ChatRoom(ctx).chatRoom(new ArrayList<ActorRef<SessionCommand>>()));
    }

    private final ActorContext<RoomCommand> context;

    private ChatRoom(ActorContext<RoomCommand> context) {
      this.context = context;
    }

    private Behavior<RoomCommand> chatRoom(List<ActorRef<SessionCommand>> sessions) {
      return Behaviors.receive(RoomCommand.class)
          .onMessage(GetSession.class, getSession -> onGetSession(sessions, getSession))
          .onMessage(PublishSessionMessage.class, pub -> onPublishSessionMessage(sessions, pub))
          .build();
    }

    private Behavior<RoomCommand> onGetSession(
        List<ActorRef<SessionCommand>> sessions, GetSession getSession)
        throws UnsupportedEncodingException {
      ActorRef<SessionEvent> client = getSession.replyTo;
      ActorRef<SessionCommand> ses =
          context.spawn(
              Session.create(context.getSelf(), getSession.screenName, client),
              URLEncoder.encode(getSession.screenName, StandardCharsets.UTF_8.name()));
      // narrow to only expose PostMessage
      client.tell(new SessionGranted(ses.narrow()));
      List<ActorRef<SessionCommand>> newSessions = new ArrayList<>(sessions);
      newSessions.add(ses);
      return chatRoom(newSessions);
    }

    private Behavior<RoomCommand> onPublishSessionMessage(
        List<ActorRef<SessionCommand>> sessions, PublishSessionMessage pub) {
      NotifyClient notification =
          new NotifyClient((new MessagePosted(pub.screenName, pub.message)));
      sessions.forEach(s -> s.tell(notification));
      return Behaviors.same();
    }

    static class Session {
      static Behavior<ChatRoom.SessionCommand> create(
          ActorRef<RoomCommand> room, String screenName, ActorRef<SessionEvent> client) {
        return Behaviors.receive(ChatRoom.SessionCommand.class)
            .onMessage(PostMessage.class, post -> onPostMessage(room, screenName, post))
            .onMessage(NotifyClient.class, notification -> onNotifyClient(client, notification))
            .build();
      }

      private static Behavior<SessionCommand> onPostMessage(
          ActorRef<RoomCommand> room, String screenName, PostMessage post) {
        // from client, publish to others via the room
        room.tell(new PublishSessionMessage(screenName, post.message));
        return Behaviors.same();
      }

      private static Behavior<SessionCommand> onNotifyClient(
          ActorRef<SessionEvent> client, NotifyClient notification) {
        // published from the room
        client.tell(notification.message);
        return Behaviors.same();
      }
    }
  }
  // #chatroom-behavior

  // #chatroom-gabbler
  public class Gabbler {
    public static Behavior<ChatRoom.SessionEvent> create() {
      return Behaviors.setup(ctx -> new Gabbler(ctx).behavior());
    }

    private final ActorContext<ChatRoom.SessionEvent> context;

    private Gabbler(ActorContext<ChatRoom.SessionEvent> context) {
      this.context = context;
    }

    private Behavior<ChatRoom.SessionEvent> behavior() {
      return Behaviors.receive(ChatRoom.SessionEvent.class)
          .onMessage(ChatRoom.SessionDenied.class, this::onSessionDenied)
          .onMessage(ChatRoom.SessionGranted.class, this::onSessionGranted)
          .onMessage(ChatRoom.MessagePosted.class, this::onMessagePosted)
          .build();
    }

    private Behavior<ChatRoom.SessionEvent> onSessionDenied(ChatRoom.SessionDenied message) {
      context.getLog().info("cannot start chat room session: {}", message.reason);
      return Behaviors.stopped();
    }

    private Behavior<ChatRoom.SessionEvent> onSessionGranted(ChatRoom.SessionGranted message) {
      message.handle.tell(new ChatRoom.PostMessage("Hello World!"));
      return Behaviors.same();
    }

    private Behavior<ChatRoom.SessionEvent> onMessagePosted(ChatRoom.MessagePosted message) {
      context
          .getLog()
          .info("message has been posted by '{}': {}", message.screenName, message.message);
      return Behaviors.stopped();
    }
  }
  // #chatroom-gabbler

  // #chatroom-main
  public class Main {
    public static Behavior<Void> create() {
      return Behaviors.setup(
          context -> {
            ActorRef<ChatRoom.RoomCommand> chatRoom = context.spawn(ChatRoom.create(), "chatRoom");
            ActorRef<ChatRoom.SessionEvent> gabbler = context.spawn(Gabbler.create(), "gabbler");
            context.watch(gabbler);
            chatRoom.tell(new ChatRoom.GetSession("ol’ Gabbler", gabbler));

            return Behaviors.receive(Void.class)
                .onSignal(Terminated.class, sig -> Behaviors.stopped())
                .build();
          });
    }

    public static void main(String[] args) {
      ActorSystem.create(Main.create(), "ChatRoomDemo");
    }
  }
  // #chatroom-main

}
