/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs21.akka.persistence.typed.javadsl;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.state.javadsl.*;

// #behavior
public class BlogPostEntityDurableState
    extends DurableStateOnCommandBehavior<
        BlogPostEntityDurableState.Command, BlogPostEntityDurableState.State> {

  public sealed interface Command {}

  public record AddPost(PostContent content, ActorRef<AddPostDone> replyTo) implements Command {}
  public record GetPost(ActorRef<PostContent> replyTo) implements Command {}
  public record ChangeBody(String newBody, ActorRef<Done> replyTo) implements Command {}
  public record Publish(ActorRef<Done> replyTo) implements Command {}
  public record PostContent(String postId, String title, String body) implements Command {}
  // reply
  public record AddPostDone(String postId) {}


  public sealed interface State {}

  public record BlankState() implements State {}

  public record DraftState(PostContent content) implements State {
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

  public record PublishedState(PostContent content) implements State {
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

  public static Behavior<Command> create(String entityId, PersistenceId persistenceId) {
    return Behaviors.setup(
        context -> {
          context.getLog().info("Starting BlogPostEntityDurableState {}", entityId);
          return new BlogPostEntityDurableState(persistenceId);
        });
  }

  private BlogPostEntityDurableState(PersistenceId persistenceId) {
    super(persistenceId);
  }

  @Override
  public State emptyState() {
    return new BlankState();
  }

  @Override
  public Effect<State> onCommand(State state, Command command) {
    return switch(state) {
      case BlankState ignored ->
        switch(command) {
          case AddPost addPost -> onAddPost(addPost);
          default -> Effect().unhandled();
        };
      case DraftState draft ->
        switch(command) {
          case ChangeBody changeBody -> onChangeBody(draft, changeBody);
          case Publish publish -> onPublish(draft, publish);
          case GetPost getPost -> onGetPost(draft, getPost);
          default -> Effect().unhandled();
        };
      case PublishedState published ->
        switch(command) {
          case ChangeBody changeBody -> onChangeBody(published, changeBody);
          case GetPost getPost -> onGetPost(published, getPost);
          default -> Effect().unhandled();
        };
    };
  }

  private Effect<State> onAddPost(AddPost cmd) {
    // #reply
    return Effect()
        .persist(new DraftState(cmd.content))
        .thenRun(() -> cmd.replyTo.tell(new AddPostDone(cmd.content.postId)));
    // #reply
  }

  private Effect<State> onChangeBody(DraftState state, ChangeBody cmd) {
    return Effect()
        .persist(state.withBody(cmd.newBody))
        .thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
  }

  private Effect<State> onChangeBody(PublishedState state, ChangeBody cmd) {
    return Effect()
        .persist(state.withBody(cmd.newBody))
        .thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
  }

  private Effect<State> onPublish(DraftState state, Publish cmd) {
    return Effect()
        .persist(new PublishedState(state.content))
        .thenRun(
            () -> {
              System.out.println("Blog post published: " + state.postId());
              cmd.replyTo.tell(Done.getInstance());
            });
  }

  private Effect<State> onGetPost(DraftState state, GetPost cmd) {
    cmd.replyTo.tell(state.content);
    return Effect().none();
  }

  private Effect<State> onGetPost(PublishedState state, GetPost cmd) {
    cmd.replyTo.tell(state.content);
    return Effect().none();
  }
}
// #behavior
