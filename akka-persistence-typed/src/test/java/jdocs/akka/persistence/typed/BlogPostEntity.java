/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;

// #behavior
public class BlogPostEntity
    extends EventSourcedBehavior<
        BlogPostEntity.Command, BlogPostEntity.Event, BlogPostEntity.State> {
  // commands, events and state as in above snippets

  // #behavior

  // #event
  public interface Event {}

  public static class PostAdded implements Event {
    private final String postId;
    private final PostContent content;

    public PostAdded(String postId, PostContent content) {
      this.postId = postId;
      this.content = content;
    }
  }

  public static class BodyChanged implements Event {
    private final String postId;
    private final String newBody;

    public BodyChanged(String postId, String newBody) {
      this.postId = postId;
      this.newBody = newBody;
    }
  }

  public static class Published implements Event {
    private final String postId;

    public Published(String postId) {
      this.postId = postId;
    }
  }
  // #event

  // #state
  interface State {}

  enum BlankState implements State {
    INSTANCE
  }

  static class DraftState implements State {
    final PostContent content;

    DraftState(PostContent content) {
      this.content = content;
    }

    DraftState withContent(PostContent newContent) {
      return new DraftState(newContent);
    }

    DraftState withBody(String newBody) {
      return withContent(new PostContent(postId(), content.title, newBody));
    }

    String postId() {
      return content.postId;
    }
  }

  static class PublishedState implements State {
    final PostContent content;

    PublishedState(PostContent content) {
      this.content = content;
    }

    PublishedState withContent(PostContent newContent) {
      return new PublishedState(newContent);
    }

    PublishedState withBody(String newBody) {
      return withContent(new PostContent(postId(), content.title, newBody));
    }

    String postId() {
      return content.postId;
    }
  }
  // #state

  // #commands
  public interface Command {}
  // #reply-command
  public static class AddPost implements Command {
    final PostContent content;
    final ActorRef<AddPostDone> replyTo;

    public AddPost(PostContent content, ActorRef<AddPostDone> replyTo) {
      this.content = content;
      this.replyTo = replyTo;
    }
  }

  public static class AddPostDone implements Command {
    final String postId;

    public AddPostDone(String postId) {
      this.postId = postId;
    }
  }
  // #reply-command
  public static class GetPost implements Command {
    final ActorRef<PostContent> replyTo;

    public GetPost(ActorRef<PostContent> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class ChangeBody implements Command {
    final String newBody;
    final ActorRef<Done> replyTo;

    public ChangeBody(String newBody, ActorRef<Done> replyTo) {
      this.newBody = newBody;
      this.replyTo = replyTo;
    }
  }

  public static class Publish implements Command {
    final ActorRef<Done> replyTo;

    public Publish(ActorRef<Done> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class PostContent implements Command {
    final String postId;
    final String title;
    final String body;

    public PostContent(String postId, String title, String body) {
      this.postId = postId;
      this.title = title;
      this.body = body;
    }
  }
  // #commands

  // #behavior
  public static Behavior<Command> create(String entityId, PersistenceId persistenceId) {
    return Behaviors.setup(
        context -> {
          context.getLog().info("Starting BlogPostEntity {}", entityId);
          return new BlogPostEntity(persistenceId);
        });
  }

  private BlogPostEntity(PersistenceId persistenceId) {
    super(persistenceId);
  }

  @Override
  public State emptyState() {
    return BlankState.INSTANCE;
  }

  // #behavior

  // #command-handler
  @Override
  public CommandHandler<Command, Event, State> commandHandler() {
    CommandHandlerBuilder<Command, Event, State> builder = newCommandHandlerBuilder();

    builder.forStateType(BlankState.class).onCommand(AddPost.class, this::onAddPost);

    builder
        .forStateType(DraftState.class)
        .onCommand(ChangeBody.class, this::onChangeBody)
        .onCommand(Publish.class, this::onPublish)
        .onCommand(GetPost.class, this::onGetPost);

    builder
        .forStateType(PublishedState.class)
        .onCommand(ChangeBody.class, this::onChangeBody)
        .onCommand(GetPost.class, this::onGetPost);

    builder.forAnyState().onCommand(AddPost.class, (state, cmd) -> Effect().unhandled());

    return builder.build();
  }

  private Effect<Event, State> onAddPost(AddPost cmd) {
    // #reply
    PostAdded event = new PostAdded(cmd.content.postId, cmd.content);
    return Effect()
        .persist(event)
        .thenRun(() -> cmd.replyTo.tell(new AddPostDone(cmd.content.postId)));
    // #reply
  }

  private Effect<Event, State> onChangeBody(DraftState state, ChangeBody cmd) {
    BodyChanged event = new BodyChanged(state.postId(), cmd.newBody);
    return Effect().persist(event).thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
  }

  private Effect<Event, State> onChangeBody(PublishedState state, ChangeBody cmd) {
    BodyChanged event = new BodyChanged(state.postId(), cmd.newBody);
    return Effect().persist(event).thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
  }

  private Effect<Event, State> onPublish(DraftState state, Publish cmd) {
    return Effect()
        .persist(new Published(state.postId()))
        .thenRun(
            () -> {
              System.out.println("Blog post published: " + state.postId());
              cmd.replyTo.tell(Done.getInstance());
            });
  }

  private Effect<Event, State> onGetPost(DraftState state, GetPost cmd) {
    cmd.replyTo.tell(state.content);
    return Effect().none();
  }

  private Effect<Event, State> onGetPost(PublishedState state, GetPost cmd) {
    cmd.replyTo.tell(state.content);
    return Effect().none();
  }
  // #command-handler

  // #event-handler
  @Override
  public EventHandler<State, Event> eventHandler() {

    EventHandlerBuilder<State, Event> builder = newEventHandlerBuilder();

    builder
        .forStateType(BlankState.class)
        .onEvent(PostAdded.class, event -> new DraftState(event.content));

    builder
        .forStateType(DraftState.class)
        .onEvent(BodyChanged.class, (state, chg) -> state.withBody(chg.newBody))
        .onEvent(Published.class, (state, event) -> new PublishedState(state.content));

    builder
        .forStateType(PublishedState.class)
        .onEvent(BodyChanged.class, (state, chg) -> state.withBody(chg.newBody));

    return builder.build();
  }
  // #event-handler

  // #behavior
  // commandHandler, eventHandler as in above snippets
}
  // #behavior
