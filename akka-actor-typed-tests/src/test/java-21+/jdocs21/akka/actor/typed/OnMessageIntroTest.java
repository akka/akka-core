/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs21.akka.actor.typed;

// #imports
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
// #imports
import akka.actor.typed.Terminated;
// #imports
import akka.actor.typed.javadsl.AbstractOnMessageBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;

import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

// #imports

public interface OnMessageIntroTest {

  // #chatroom-behavior
  public class ChatRoom {
    public sealed interface RoomCommand {}
    public record GetSession(String screenName, ActorRef<SessionEvent> replyTo) implements RoomCommand {}
    // #chatroom-protocol
    private record PublishSessionMessage(String screenName, String message) implements RoomCommand {}
    // #chatroom-protocol

    sealed interface SessionEvent {}
    public record SessionGranted(ActorRef<PostMessage> handle) implements SessionEvent {}
    public record SessionDenied(String reason) implements SessionEvent {}
    public record MessagePosted(String screenName, String message) implements SessionEvent {}

    sealed interface SessionCommand {}
    public record PostMessage(String message) implements SessionCommand {}
    private record NotifyClient(MessagePosted message) implements SessionCommand {}
    // #chatroom-protocol

    public static Behavior<RoomCommand> create() {
      return Behaviors.setup(ChatRoomBehavior::new);
    }

    private static class ChatRoomBehavior extends AbstractOnMessageBehavior<RoomCommand> {

      private final List<ActorRef<SessionCommand>> sessions = new ArrayList<>();

      private ChatRoomBehavior(ActorContext<RoomCommand> context) {
        super(context);
      }

      @Override
      public Behavior<RoomCommand> onMessage(RoomCommand msg) {
        return switch(msg) {
          case GetSession gs -> onGetSession(gs);
          case PublishSessionMessage psm -> onPublishSessionMessage(psm);
        };
      }

      private Behavior<RoomCommand> onGetSession(GetSession gs) {
        ActorRef<SessionEvent> client = gs.replyTo;
        ActorRef<SessionCommand> ses =
            getContext()
                .spawn(
                    SessionBehavior.create(getContext().getSelf(), gs.screenName, client),
                    URLEncoder.encode(gs.screenName, StandardCharsets.UTF_8));

        // narrow to only expose PostMessage
        client.tell(new SessionGranted(ses.narrow()));
        sessions.add(ses);

        return this;
      }

      private Behavior<RoomCommand> onPublishSessionMessage(PublishSessionMessage pub) {
        NotifyClient notification =
            new NotifyClient(new MessagePosted(pub.screenName, pub.message));

        sessions.forEach(s -> s.tell(notification));
        return this;
      }
    }

    // #chatroom-behavior
    private static class SessionBehavior extends AbstractOnMessageBehavior<SessionCommand> {
      private final ActorRef<RoomCommand> room;
      private final String screenName;
      private final ActorRef<SessionEvent> client;

      public static Behavior<SessionCommand> create(
          ActorRef<RoomCommand> room, String screenName, ActorRef<SessionEvent> client) {
        return Behaviors.setup(context -> new SessionBehavior(context, room, screenName, client));
      }

      private SessionBehavior(
          ActorContext<SessionCommand> context,
          ActorRef<RoomCommand> room,
          String screenName,
          ActorRef<SessionEvent> client) {
        super(context);
        this.room = room;
        this.screenName = screenName;
        this.client = client;
      }

      @Override
      public Behavior<SessionCommand> onMessage(SessionCommand msg) {
        switch(msg) {
          case PostMessage post ->
            // from client, publish to others via the room
            room.tell(new PublishSessionMessage(screenName, post.message));
        case NotifyClient notifyClient ->
            // published from the room
            client.tell(notifyClient.message);
        }
        return this;
      }
    }
    // #chatroom-behavior
  }
  // #chatroom-behavior

  // NB: leaving the gabbler as an AbstractBehavior, as the point should be made by now
  // #chatroom-gabbler
  public class Gabbler extends AbstractOnMessageBehavior<ChatRoom.SessionEvent> {
    public static Behavior<ChatRoom.SessionEvent> create() {
      return Behaviors.setup(Gabbler::new);
    }

    private Gabbler(ActorContext<ChatRoom.SessionEvent> context) {
      super(context);
    }

    @Override
    public Behavior<ChatRoom.SessionEvent> onMessage(ChatRoom.SessionEvent message) throws Exception {
      return switch(message) {
        case ChatRoom.SessionDenied denied -> onSessionDenied(denied);
        case ChatRoom.SessionGranted granted -> onSessionGranted(granted);
        case ChatRoom.MessagePosted posted -> onMessagePosted(posted);
      };
    }

    private Behavior<ChatRoom.SessionEvent> onSessionDenied(ChatRoom.SessionDenied message) {
      getContext().getLog().info("cannot start chat room session: {}", message.reason);
      return Behaviors.stopped();
    }

    private Behavior<ChatRoom.SessionEvent> onSessionGranted(ChatRoom.SessionGranted message) {
      message.handle.tell(new ChatRoom.PostMessage("Hello World!"));
      return Behaviors.same();
    }

    private Behavior<ChatRoom.SessionEvent> onMessagePosted(ChatRoom.MessagePosted message) {
      getContext()
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
