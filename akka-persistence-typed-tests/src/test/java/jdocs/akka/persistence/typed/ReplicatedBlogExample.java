/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.crdt.LwwTime;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.Effect;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicatedEventSourcing;
import akka.persistence.typed.javadsl.ReplicationContext;
import java.util.Optional;
import java.util.Set;

interface ReplicatedBlogExample {

  public final class BlogEntity
      extends ReplicatedEventSourcedBehavior<
          BlogEntity.Command, BlogEntity.Event, BlogEntity.BlogState> {

    private final ActorContext<Command> context;

    interface Command {
      String getPostId();
    }

    static final class AddPost implements Command {
      final String postId;
      final PostContent content;
      final ActorRef<AddPostDone> replyTo;

      public AddPost(String postId, PostContent content, ActorRef<AddPostDone> replyTo) {
        this.postId = postId;
        this.content = content;
        this.replyTo = replyTo;
      }

      public String getPostId() {
        return postId;
      }
    }

    static final class AddPostDone {
      final String postId;

      AddPostDone(String postId) {
        this.postId = postId;
      }

      public String getPostId() {
        return postId;
      }
    }

    static final class GetPost implements Command {
      final String postId;
      final ActorRef<PostContent> replyTo;

      public GetPost(String postId, ActorRef<PostContent> replyTo) {
        this.postId = postId;
        this.replyTo = replyTo;
      }

      public String getPostId() {
        return postId;
      }
    }

    static final class ChangeBody implements Command {
      final String postId;
      final PostContent newContent;
      final ActorRef<Done> replyTo;

      public ChangeBody(String postId, PostContent newContent, ActorRef<Done> replyTo) {
        this.postId = postId;
        this.newContent = newContent;
        this.replyTo = replyTo;
      }

      public String getPostId() {
        return postId;
      }
    }

    static final class Publish implements Command {
      final String postId;
      final ActorRef<Done> replyTo;

      public Publish(String postId, ActorRef<Done> replyTo) {
        this.postId = postId;
        this.replyTo = replyTo;
      }

      public String getPostId() {
        return postId;
      }
    }

    interface Event {}

    static final class PostAdded implements Event {
      final String postId;
      final PostContent content;
      final LwwTime timestamp;

      public PostAdded(String postId, PostContent content, LwwTime timestamp) {
        this.postId = postId;
        this.content = content;
        this.timestamp = timestamp;
      }
    }

    static final class BodyChanged implements Event {
      final String postId;
      final PostContent content;
      final LwwTime timestamp;

      public BodyChanged(String postId, PostContent content, LwwTime timestamp) {
        this.postId = postId;
        this.content = content;
        this.timestamp = timestamp;
      }
    }

    static final class Published implements Event {
      final String postId;

      public Published(String postId) {
        this.postId = postId;
      }
    }

    public static final class PostContent {
      final String title;
      final String body;

      public PostContent(String title, String body) {
        this.title = title;
        this.body = body;
      }
    }

    public static class BlogState {

      public static final BlogState EMPTY =
          new BlogState(Optional.empty(), new LwwTime(Long.MIN_VALUE, ReplicaId.empty()), false);

      final Optional<PostContent> content;
      final LwwTime contentTimestamp;
      final boolean published;

      public BlogState(Optional<PostContent> content, LwwTime contentTimestamp, boolean published) {
        this.content = content;
        this.contentTimestamp = contentTimestamp;
        this.published = published;
      }

      BlogState withContent(PostContent newContent, LwwTime timestamp) {
        return new BlogState(Optional.of(newContent), timestamp, this.published);
      }

      BlogState publish() {
        if (published) {
          return this;
        } else {
          return new BlogState(content, contentTimestamp, true);
        }
      }

      boolean isEmpty() {
        return !content.isPresent();
      }
    }

    public static Behavior<Command> create(
        String entityId, ReplicaId replicaId, Set<ReplicaId> allReplicas) {
      return Behaviors.setup(
          context ->
              ReplicatedEventSourcing.commonJournalConfig(
                  new ReplicationId("blog", entityId, replicaId),
                  allReplicas,
                  PersistenceTestKitReadJournal.Identifier(),
                  replicationContext -> new BlogEntity(context, replicationContext)));
    }

    private BlogEntity(ActorContext<Command> context, ReplicationContext replicationContext) {
      super(replicationContext);
      this.context = context;
    }

    @Override
    public BlogState emptyState() {
      return BlogState.EMPTY;
    }

    // #command-handler
    @Override
    public CommandHandler<Command, Event, BlogState> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(AddPost.class, this::onAddPost)
          .onCommand(ChangeBody.class, this::onChangeBody)
          .onCommand(Publish.class, this::onPublish)
          .onCommand(GetPost.class, this::onGetPost)
          .build();
    }

    private Effect<Event, BlogState> onAddPost(BlogState state, AddPost command) {
      PostAdded evt =
          new PostAdded(
              getReplicationContext().entityId(),
              command.content,
              state.contentTimestamp.increase(
                  getReplicationContext().currentTimeMillis(),
                  getReplicationContext().replicaId()));
      return Effect()
          .persist(evt)
          .thenRun(() -> command.replyTo.tell(new AddPostDone(getReplicationContext().entityId())));
    }

    private Effect<Event, BlogState> onChangeBody(BlogState state, ChangeBody command) {
      BodyChanged evt =
          new BodyChanged(
              getReplicationContext().entityId(),
              command.newContent,
              state.contentTimestamp.increase(
                  getReplicationContext().currentTimeMillis(),
                  getReplicationContext().replicaId()));
      return Effect().persist(evt).thenRun(() -> command.replyTo.tell(Done.getInstance()));
    }

    private Effect<Event, BlogState> onPublish(BlogState state, Publish command) {
      Published evt = new Published(getReplicationContext().entityId());
      return Effect().persist(evt).thenRun(() -> command.replyTo.tell(Done.getInstance()));
    }

    private Effect<Event, BlogState> onGetPost(BlogState state, GetPost command) {
      context.getLog().info("GetPost {}", state.content);
      if (state.content.isPresent()) command.replyTo.tell(state.content.get());
      return Effect().none();
    }
    // #command-handler

    // #event-handler
    @Override
    public EventHandler<BlogState, Event> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onEvent(PostAdded.class, this::onPostAdded)
          .onEvent(BodyChanged.class, this::onBodyChanged)
          .onEvent(Published.class, this::onPublished)
          .build();
    }

    private BlogState onPostAdded(BlogState state, PostAdded event) {
      if (event.timestamp.isAfter(state.contentTimestamp)) {
        BlogState s = state.withContent(event.content, event.timestamp);
        context.getLog().info("Updating content. New content is {}", s);
        return s;
      } else {
        context.getLog().info("Ignoring event as timestamp is older");
        return state;
      }
    }

    private BlogState onBodyChanged(BlogState state, BodyChanged event) {
      if (event.timestamp.isAfter(state.contentTimestamp)) {
        return state.withContent(event.content, event.timestamp);
      } else {
        return state;
      }
    }

    private BlogState onPublished(BlogState state, Published event) {
      return state.publish();
    }
    // #event-handler

  }
}
