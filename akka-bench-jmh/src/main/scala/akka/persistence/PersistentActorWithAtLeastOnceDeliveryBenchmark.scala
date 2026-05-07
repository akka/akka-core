/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.commons.io.FileUtils
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.Scope

import akka.actor._
import akka.testkit.TestProbe

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
class PersistentActorWithAtLeastOnceDeliveryBenchmark {

  val config = PersistenceSpec.config("leveldb", "benchmark")

  lazy val storageLocations =
    List(
      "akka.persistence.journal.leveldb.dir",
      "akka.persistence.journal.leveldb-shared.store.dir",
      "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  var system: ActorSystem = _

  var probe: TestProbe = _
  var actor: ActorRef = _
  var persistPersistentActorWithAtLeastOnceDelivery: ActorRef = _
  var persistAsyncPersistentActorWithAtLeastOnceDelivery: ActorRef = _
  var noPersistPersistentActorWithAtLeastOnceDelivery: ActorRef = _
  var destinationActor: ActorRef = _

  val dataCount = 10000

  @Setup
  def setup(): Unit = {
    system = ActorSystem("PersistentActorWithAtLeastOnceDeliveryBenchmark", config)

    probe = TestProbe()(system)

    storageLocations.foreach(FileUtils.deleteDirectory)

    destinationActor = system.actorOf(Props[DestinationActor](), "destination")

    noPersistPersistentActorWithAtLeastOnceDelivery = system.actorOf(
      Props(classOf[NoPersistPersistentActorWithAtLeastOnceDelivery], dataCount, probe.ref, destinationActor.path),
      "nop-1")
    persistPersistentActorWithAtLeastOnceDelivery = system.actorOf(
      Props(classOf[PersistPersistentActorWithAtLeastOnceDelivery], dataCount, probe.ref, destinationActor.path),
      "ep-1")
    persistAsyncPersistentActorWithAtLeastOnceDelivery = system.actorOf(
      Props(classOf[PersistAsyncPersistentActorWithAtLeastOnceDelivery], dataCount, probe.ref, destinationActor.path),
      "epa-1")
  }

  @TearDown
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)

    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_persistAsync_with_AtLeastOnceDelivery(): Unit = {
    for (i <- 1 to dataCount)
      persistAsyncPersistentActorWithAtLeastOnceDelivery.tell(i, probe.ref)
    probe.expectMsg(20.seconds, Evt(dataCount))
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_persist_with_AtLeastOnceDelivery(): Unit = {
    for (i <- 1 to dataCount)
      persistPersistentActorWithAtLeastOnceDelivery.tell(i, probe.ref)
    probe.expectMsg(2.minutes, Evt(dataCount))
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_noPersist_with_AtLeastOnceDelivery(): Unit = {
    for (i <- 1 to dataCount)
      noPersistPersistentActorWithAtLeastOnceDelivery.tell(i, probe.ref)
    probe.expectMsg(20.seconds, Evt(dataCount))
  }
}

class NoPersistPersistentActorWithAtLeastOnceDelivery(
    respondAfter: Int,
    val upStream: ActorRef,
    val downStream: ActorPath)
    extends PersistentActor
    with AtLeastOnceDelivery {

  override def redeliverInterval = 100.milliseconds

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      deliver(downStream)(deliveryId => Msg(deliveryId, n))
      if (n == respondAfter)
        //switch to wait all message confirmed
        context.become(waitConfirm)
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
    case _ => // do nothing
  }

  override def receiveRecover = {
    case _ => // do nothing
  }

  val waitConfirm: Actor.Receive = {
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
      if (numberOfUnconfirmed == 0) {
        upStream ! Evt(respondAfter)
        context.unbecome()
      }
    case _ => // do nothing
  }
}

class PersistPersistentActorWithAtLeastOnceDelivery(
    respondAfter: Int,
    val upStream: ActorRef,
    val downStream: ActorPath)
    extends PersistentActor
    with AtLeastOnceDelivery {

  override def redeliverInterval = 100.milliseconds

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persist(MsgSent(n)) { _ =>
        deliver(downStream)(deliveryId => Msg(deliveryId, n))
        if (n == respondAfter)
          //switch to wait all message confirmed
          context.become(waitConfirm)
      }
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
    case _ => // do nothing
  }

  override def receiveRecover = {
    case _ => // do nothing
  }

  val waitConfirm: Actor.Receive = {
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
      if (numberOfUnconfirmed == 0) {
        upStream ! Evt(respondAfter)
        context.unbecome()
      }
    case _ => // do nothing
  }
}

class PersistAsyncPersistentActorWithAtLeastOnceDelivery(
    respondAfter: Int,
    val upStream: ActorRef,
    val downStream: ActorPath)
    extends PersistentActor
    with AtLeastOnceDelivery {

  override def redeliverInterval = 100.milliseconds

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persistAsync(MsgSent(n)) { _ =>
        deliver(downStream)(deliveryId => Msg(deliveryId, n))
        if (n == respondAfter)
          //switch to wait all message confirmed
          context.become(waitConfirm)
      }
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
    case _ => // do nothing
  }

  override def receiveRecover = {
    case _ => // do nothing
  }

  val waitConfirm: Actor.Receive = {
    case Confirm(deliveryId) =>
      confirmDelivery(deliveryId)
      if (numberOfUnconfirmed == 0) {
        upStream ! Evt(respondAfter)
        context.unbecome()
      }
    case _ => // do nothing
  }
}

case class Msg(deliveryId: Long, n: Int)

case class Confirm(deliveryId: Long)

sealed trait Event

case class MsgSent(n: Int) extends Event

case class MsgConfirmed(deliveryId: Long) extends Event

class DestinationActor extends Actor {
  var seqNr = 0L

  override def receive = {
    case n: Int =>
      sender() ! Confirm(n)
    case Msg(deliveryId, _) =>
      seqNr += 1
      if (seqNr % 11 == 0) {
        //drop it
      } else {
        sender() ! Confirm(deliveryId)
      }
    case _ => // do nothing
  }
}
