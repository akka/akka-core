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

/*
  # OS:   OSX 10.9.3
  # CPU:  Intel(R) Core(TM) i7-4850HQ CPU @ 2.30GHz
  # Date: Mon Jun  9 13:22:42 CEST 2014

  [info] Benchmark                                                                            Mode   Samples         Mean   Mean error    Units
  [info] a.p.PersistentActorDeferBenchmark.tell_persistAsync_defer_persistAsync_reply        thrpt        10        6.858        0.515   ops/ms
  [info] a.p.PersistentActorDeferBenchmark.tell_persistAsync_defer_persistAsync_replyASAP    thrpt        10       20.256        2.941   ops/ms
  [info] a.p.PersistentActorDeferBenchmark.tell_processor_Persistent_reply                   thrpt        10        6.531        0.114   ops/ms
  [info] a.p.PersistentActorDeferBenchmark.tell_processor_Persistent_replyASAP               thrpt        10       26.000        0.694   ops/ms
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
class PersistentActorDeferBenchmark {

  val config = PersistenceSpec.config("leveldb", "benchmark")

  lazy val storageLocations =
    List(
      "akka.persistence.journal.leveldb.dir",
      "akka.persistence.journal.leveldb-shared.store.dir",
      "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  var system: ActorSystem = _

  var probe: TestProbe = _
  var persistAsync_defer: ActorRef = _
  var persistAsync_defer_replyASAP: ActorRef = _

  val data10k = (1 to 10000).toArray

  @Setup
  def setup(): Unit = {
    system = ActorSystem("test", config)

    probe = TestProbe()(system)

    storageLocations.foreach(FileUtils.deleteDirectory)
    persistAsync_defer = system.actorOf(Props(classOf[`persistAsync, defer`], data10k.last), "a-1")
    persistAsync_defer_replyASAP =
      system.actorOf(Props(classOf[`persistAsync, defer, respond ASAP`], data10k.last), "a-2")
  }

  @TearDown
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)

    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def tell_persistAsync_defer_persistAsync_reply(): Unit = {
    for (i <- data10k) persistAsync_defer.tell(i, probe.ref)

    probe.expectMsg(data10k.last)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def tell_persistAsync_defer_persistAsync_replyASAP(): Unit = {
    for (i <- data10k) persistAsync_defer_replyASAP.tell(i, probe.ref)

    probe.expectMsg(data10k.last)
  }

}

class `persistAsync, defer`(respondAfter: Int) extends PersistentActor {

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persistAsync(Evt(n)) { _ =>
      }
      deferAsync(Evt(n)) { e =>
        if (e.i == respondAfter) sender() ! e.i
      }
  }
  override def receiveRecover = {
    case _ => // do nothing
  }
}
class `persistAsync, defer, respond ASAP`(respondAfter: Int) extends PersistentActor {

  override def persistenceId: String = self.path.name

  override def receiveCommand = {
    case n: Int =>
      persistAsync(Evt(n)) { _ =>
      }
      deferAsync(Evt(n)) { _ =>
      }
      if (n == respondAfter) sender() ! n
  }
  override def receiveRecover = {
    case _ => // do nothing
  }
}
