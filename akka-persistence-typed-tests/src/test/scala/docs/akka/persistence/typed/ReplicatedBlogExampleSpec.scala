/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.crdt.LwwTime
import akka.persistence.typed.scaladsl._
import akka.serialization.jackson.CborSerializable

object ReplicatedBlogExampleSpec {

  object BlogEntity {

    object BlogState {
      val empty: BlogState = BlogState(None, LwwTime(Long.MinValue, ReplicaId.empty), published = false)
    }
    final case class BlogState(content: Option[PostContent], contentTimestamp: LwwTime, published: Boolean)
        extends CborSerializable {
      def withContent(newContent: PostContent, timestamp: LwwTime): BlogState =
        copy(content = Some(newContent), contentTimestamp = timestamp)

      def isEmpty: Boolean = content.isEmpty
    }

    final case class PostContent(title: String, body: String) extends CborSerializable
    final case class Published(postId: String) extends Event

    sealed trait Command extends CborSerializable
    final case class AddPost(postId: String, content: PostContent, replyTo: ActorRef[AddPostDone]) extends Command
    final case class AddPostDone(postId: String)
    final case class GetPost(postId: String, replyTo: ActorRef[PostContent]) extends Command
    final case class ChangeBody(postId: String, newContent: PostContent, replyTo: ActorRef[Done]) extends Command
    final case class Publish(postId: String, replyTo: ActorRef[Done]) extends Command

    sealed trait Event extends CborSerializable
    final case class PostAdded(postId: String, content: PostContent, timestamp: LwwTime) extends Event
    final case class BodyChanged(postId: String, newContent: PostContent, timestamp: LwwTime) extends Event

    def apply(entityId: String, replicaId: ReplicaId, allReplicaIds: Set[ReplicaId]): Behavior[Command] = {
      Behaviors.setup[Command] { ctx =>
        ReplicatedEventSourcing.commonJournalConfig(
          ReplicationId("blog", entityId, replicaId),
          allReplicaIds,
          PersistenceTestKitReadJournal.Identifier) { replicationContext =>
          EventSourcedBehavior[Command, Event, BlogState](
            replicationContext.persistenceId,
            BlogState.empty,
            (state, cmd) => commandHandler(ctx, replicationContext, state, cmd),
            (state, event) => eventHandler(ctx, replicationContext, state, event))
        }
      }
    }

    //#command-handler
    private def commandHandler(
        ctx: ActorContext[Command],
        replicationContext: ReplicationContext,
        state: BlogState,
        cmd: Command): Effect[Event, BlogState] = {
      cmd match {
        case AddPost(_, content, replyTo) =>
          val evt =
            PostAdded(
              replicationContext.entityId,
              content,
              state.contentTimestamp.increase(replicationContext.currentTimeMillis(), replicationContext.replicaId))
          Effect.persist(evt).thenRun { _ =>
            replyTo ! AddPostDone(replicationContext.entityId)
          }
        case ChangeBody(_, newContent, replyTo) =>
          val evt =
            BodyChanged(
              replicationContext.entityId,
              newContent,
              state.contentTimestamp.increase(replicationContext.currentTimeMillis(), replicationContext.replicaId))
          Effect.persist(evt).thenRun { _ =>
            replyTo ! Done
          }
        case p: Publish =>
          Effect.persist(Published("id")).thenRun { _ =>
            p.replyTo ! Done
          }
        case gp: GetPost =>
          ctx.log.info("GetPost {}", state.content)
          state.content.foreach(content => gp.replyTo ! content)
          Effect.none
      }
    }
    //#command-handler

    //#event-handler
    private def eventHandler(
        ctx: ActorContext[Command],
        replicationContext: ReplicationContext,
        state: BlogState,
        event: Event): BlogState = {
      ctx.log.info(s"${replicationContext.entityId}:${replicationContext.replicaId} Received event $event")
      event match {
        case PostAdded(_, content, timestamp) =>
          if (timestamp.isAfter(state.contentTimestamp)) {
            val s = state.withContent(content, timestamp)
            ctx.log.info("Updating content. New content is {}", s)
            s
          } else {
            ctx.log.info("Ignoring event as timestamp is older")
            state
          }
        case BodyChanged(_, newContent, timestamp) =>
          if (timestamp.isAfter(state.contentTimestamp))
            state.withContent(newContent, timestamp)
          else state
        case Published(_) =>
          state.copy(published = true)
      }
    }
    //#event-handler
  }
}

class ReplicatedBlogExampleSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReplicatedBlogExampleSpec.BlogEntity
  import ReplicatedBlogExampleSpec.BlogEntity._

  "Blog Example" should {
    "work" in {
      val refDcA: ActorRef[Command] =
        spawn(BlogEntity("cat", ReplicaId("DC-A"), Set(ReplicaId("DC-A"), ReplicaId("DC-B"))))

      val refDcB: ActorRef[Command] =
        spawn(BlogEntity("cat", ReplicaId("DC-B"), Set(ReplicaId("DC-A"), ReplicaId("DC-B"))))

      import scala.concurrent.duration._

      import akka.actor.typed.scaladsl.AskPattern._
      import akka.util.Timeout
      implicit val timeout: Timeout = 3.seconds

      val content = PostContent("cats are the bets", "yep")
      val response =
        refDcA.ask[AddPostDone](replyTo => AddPost("cat", content, replyTo)).futureValue

      response shouldEqual AddPostDone("cat")

      eventually {
        refDcA.ask[PostContent](replyTo => GetPost("cat", replyTo)).futureValue shouldEqual content
      }

      eventually {
        refDcB.ask[PostContent](replyTo => GetPost("cat", replyTo)).futureValue shouldEqual content
      }
    }
  }
}
