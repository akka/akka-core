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
class PersistentActorThroughputBenchmark {

  val config = PersistenceSpec.config("leveldb", "benchmark")

  lazy val storageLocations =
    List(
      "akka.persistence.journal.leveldb.dir",
      "akka.persistence.journal.leveldb-shared.store.dir",
      "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  var system: ActorSystem = _

  var probe: TestProbe = _
  var actor: ActorRef = _
  var persistPersistentActor: ActorRef = _
  var persistAsync1PersistentActor: ActorRef = _
  var noPersistPersistentActor: ActorRef = _
  var persistAsyncQuickReplyPersistentActor: ActorRef = _

  val data10k = (1 to 10000).toArray

  @Setup
  def setup(): Unit = {
    system = ActorSystem("test", config)

    probe = TestProbe()(system)

    storageLocations.foreach(FileUtils.deleteDirectory)

    actor = system.actorOf(Props(classOf[BaselineActor], data10k.last), "a-1")

    noPersistPersistentActor = system.actorOf(Props(classOf[NoPersistPersistentActor], data10k.last), "nop-1")
    persistPersistentActor = system.actorOf(Props(classOf[PersistPersistentActor], data10k.last), "ep-1")
    persistAsync1PersistentActor = system.actorOf(Props(classOf[PersistAsyncPersistentActor], data10k.last), "epa-1")

    persistAsyncQuickReplyPersistentActor =
      system.actorOf(Props(classOf[PersistAsyncQuickReplyPersistentActor], data10k.last), "epa-2")
  }

  @TearDown
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)

    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def actor_normalActor_reply_baseline(): Unit = {
    for (i <- data10k) actor.tell(i, probe.ref)

    probe.expectMsg(data10k.last)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_persist_reply(): Unit = {
    for (i <- data10k) persistPersistentActor.tell(i, probe.ref)

    probe.expectMsg(Evt(data10k.last))
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_persistAsync_reply(): Unit = {
    for (i <- data10k) persistAsync1PersistentActor.tell(i, probe.ref)

    probe.expectMsg(Evt(data10k.last))
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_noPersist_reply(): Unit = {
    for (i <- data10k) noPersistPersistentActor.tell(i, probe.ref)

    probe.expectMsg(Evt(data10k.last))
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def persistentActor_persistAsync_replyRightOnCommandReceive(): Unit = {
    for (i <- data10k) persistAsyncQuickReplyPersistentActor.tell(i, probe.ref)

    probe.expectMsg(Evt(data10k.last))
  }

}

class NoPersistPersistentActor(respondAfter: Int) extends PersistentActor {

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int => if (n == respondAfter) sender() ! Evt(n)
  }
  override def receiveRecover = {
    case _ => // do nothing
  }

}
class PersistPersistentActor(respondAfter: Int) extends PersistentActor {

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persist(Evt(n)) { e =>
        if (e.i == respondAfter) sender() ! e
      }
  }
  override def receiveRecover = {
    case _ => // do nothing
  }

}

class PersistAsyncPersistentActor(respondAfter: Int) extends PersistentActor {
  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persistAsync(Evt(n)) { e =>
        if (e.i == respondAfter) sender() ! e
      }
  }
  override def receiveRecover = {
    case _ => // do nothing
  }
}

class PersistAsyncQuickReplyPersistentActor(respondAfter: Int) extends PersistentActor {

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      val e = Evt(n)
      if (n == respondAfter) sender() ! e
      persistAsync(e)(identity)
  }
  override def receiveRecover = {
    case _ => // do nothing
  }
}
