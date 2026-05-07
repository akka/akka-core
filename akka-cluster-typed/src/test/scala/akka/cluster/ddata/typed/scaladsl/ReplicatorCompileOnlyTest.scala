/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.Scheduler
import akka.cluster.ddata.GCounter
import akka.cluster.ddata.GCounterKey
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.typed.scaladsl.Replicator._
import akka.util.Timeout

object ReplicatorCompileOnlyTest {
  sealed trait ClientCommand
  private sealed trait InternalMsg extends ClientCommand
  private case class InternalUpdateResponse(rsp: Replicator.UpdateResponse[GCounter]) extends InternalMsg
  private case class InternalGetResponse(rsp: Replicator.GetResponse[GCounter], replyTo: ActorRef[Int])
      extends InternalMsg

  def shouldHaveConvenienceForAsk(): Unit = {
    import akka.actor.typed.scaladsl.AskPattern._

    val replicator: ActorRef[Replicator.Command] = ???
    implicit val timeout: Timeout = Timeout(3.seconds)
    implicit val scheduler: Scheduler = ???
    implicit val cluster: SelfUniqueAddress = ???
    val key = GCounterKey("counter")

    val reply1: Future[GetResponse[GCounter]] = replicator.ask(Replicator.Get(key, Replicator.ReadLocal))

    val reply2: Future[UpdateResponse[GCounter]] =
      replicator.ask(Replicator.Update(key, GCounter.empty, Replicator.WriteLocal)(_ :+ 1))

    val reply3: Future[DeleteResponse[GCounter]] = replicator.ask(Replicator.Delete(key, Replicator.WriteLocal))

    val reply4: Future[ReplicaCount] = replicator.ask(Replicator.GetReplicaCount())

    // suppress unused compiler warnings
    println("" + reply1 + reply2 + reply3 + reply4)
  }

  def shouldHaveConvenienceForAsk2(): Unit = {
    implicit val cluster: SelfUniqueAddress = ???
    val replicatorAdapter: ReplicatorMessageAdapter[ClientCommand, GCounter] = ???
    val replyTo: ActorRef[Int] = ???
    val key = GCounterKey("counter")

    //#curried-update
    // alternative way to define the `createRequest` function
    // Replicator.Update instance has a curried `apply` method
    replicatorAdapter.askUpdate(
      Replicator.Update(key, GCounter.empty, Replicator.WriteLocal)(_ :+ 1),
      InternalUpdateResponse.apply)

    // that is the same as
    replicatorAdapter.askUpdate(
      askReplyTo => Replicator.Update(key, GCounter.empty, Replicator.WriteLocal, askReplyTo)(_ :+ 1),
      InternalUpdateResponse.apply)
    //#curried-update

    //#curried-get
    // alternative way to define the `createRequest` function
    // Replicator.Get instance has a curried `apply` method
    replicatorAdapter.askGet(Replicator.Get(key, Replicator.ReadLocal), value => InternalGetResponse(value, replyTo))

    // that is the same as
    replicatorAdapter.askGet(
      askReplyTo => Replicator.Get(key, Replicator.ReadLocal, askReplyTo),
      value => InternalGetResponse(value, replyTo))
    //#curried-get
  }

  def shouldHaveUnapplyForResponseTypes(): Unit = {
    val getResponse: GetResponse[GCounter] = ???
    val key = GCounterKey("counter")

    getResponse match {
      case GetSuccess(`key`)     =>
      case GetFailure(`key`)     =>
      case NotFound(`key`)       =>
      case GetDataDeleted(`key`) =>
      case unexpected            => throw new RuntimeException(s"Unexpected: $unexpected")
    }

    val updateResponse: UpdateResponse[GCounter] = ???
    updateResponse match {
      case UpdateSuccess(`key`)       =>
      case ModifyFailure(`key`, _, _) =>
      case UpdateTimeout(`key`)       =>
      case StoreFailure(`key`)        =>
      case UpdateFailure(`key`)       =>
      case UpdateDataDeleted(`key`)   =>
      case unexpected                 => throw new RuntimeException(s"Unexpected: $unexpected")
    }

    val deleteResponse: DeleteResponse[GCounter] = ???
    deleteResponse match {
      case DeleteSuccess(`key`) =>
      case DeleteFailure(`key`) =>
      case DataDeleted(`key`)   =>
      case unexpected           => throw new RuntimeException(s"Unexpected: $unexpected")
    }

    val subscribeResponse: SubscribeResponse[GCounter] = ???
    subscribeResponse match {
      case Changed(`key`) =>
      case Deleted(`key`) =>
      case unexpected     => throw new RuntimeException(s"Unexpected: $unexpected")
    }

    val replicaCount: ReplicaCount = ???
    replicaCount match {
      case ReplicaCount(_) =>
      case unexpected      => throw new RuntimeException(s"Unexpected: $unexpected")
    }
  }

  def shouldHaveApplyForConsistencies(): Unit = {
    Replicator.ReadFrom(3, 3.seconds)
    Replicator.ReadMajority(3.seconds)
    Replicator.ReadMajority(3.seconds, minCap = 5)
    Replicator.ReadAll(3.seconds)

    Replicator.WriteTo(3, 3.seconds)
    Replicator.WriteMajority(3.seconds)
    Replicator.WriteMajority(3.seconds, minCap = 5)
    Replicator.WriteAll(3.seconds)
  }
}
