/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.commons.io.FileUtils
import org.openjdk.jmh.annotations._

import akka.actor._
import akka.persistence.journal.AsyncWriteTarget._
import akka.persistence.journal.leveldb.{ SharedLeveldbJournal, SharedLeveldbStore }
import akka.testkit.TestProbe

/*
  # OS:   OSX 10.9.3
  # CPU:  Intel(R) Core(TM) i7-4850HQ CPU @ 2.30GHz
  # Date: Mon Jul 23 11:07:42 CEST 2014

  This bench emulates what we provide with "Processor batching".
  As expected, batching writes is better than writing 1 by 1.
  The important thing though is that there didn't appear to be any "write latency spikes" throughout this bench.

[info] Benchmark                                       Mode   Samples        Score  Score error    Units
[info] a.p.LevelDbBatchingBenchmark.write_1            avgt        20        0.799        0.011    ms/op
[info] a.p.LevelDbBatchingBenchmark.writeBatch_10      avgt        20        0.117        0.001    ms/op
[info] a.p.LevelDbBatchingBenchmark.writeBatch_100     avgt        20        0.050        0.000    ms/op
[info] a.p.LevelDbBatchingBenchmark.writeBatch_200     avgt        20        0.041        0.001    ms/op
 */
@Fork(1)
@Threads(10)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class LevelDbBatchingBenchmark {

  var sys: ActorSystem = _
  var probe: TestProbe = _
  var store: ActorRef = _

  val batch_1 = List.fill(1) { AtomicWrite(PersistentRepr("data", 12, "pa")) }
  val batch_10 = List.fill(10) { AtomicWrite(PersistentRepr("data", 12, "pa")) }
  val batch_100 = List.fill(100) { AtomicWrite(PersistentRepr("data", 12, "pa")) }
  val batch_200 = List.fill(200) { AtomicWrite(PersistentRepr("data", 12, "pa")) }

  @Setup(Level.Trial)
  @nowarn("msg=deprecated")
  def setup(): Unit = {
    sys = ActorSystem("sys")
    deleteStorage(sys)
    SharedLeveldbJournal.setStore(store, sys)

    probe = TestProbe()(sys)
    store = sys.actorOf(Props[SharedLeveldbStore](), "store")
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    store ! PoisonPill
    Thread.sleep(500)

    sys.terminate()
    Await.ready(sys.whenTerminated, 10.seconds)
  }

  @Benchmark
  @Measurement(timeUnit = TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(1)
  def write_1(): Unit = {
    probe.send(store, WriteMessages(batch_1))
    probe.expectMsgType[Any]
  }

  @Benchmark
  @Measurement(timeUnit = TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(10)
  def writeBatch_10(): Unit = {
    probe.send(store, WriteMessages(batch_10))
    probe.expectMsgType[Any]
  }

  @Benchmark
  @Measurement(timeUnit = TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(100)
  def writeBatch_100(): Unit = {
    probe.send(store, WriteMessages(batch_100))
    probe.expectMsgType[Any]
  }

  @Benchmark
  @Measurement(timeUnit = TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(200)
  def writeBatch_200(): Unit = {
    probe.send(store, WriteMessages(batch_200))
    probe.expectMsgType[Any]
  }

  // TOOLS

  private def deleteStorage(sys: ActorSystem): Unit = {
    val storageLocations =
      List(
        "akka.persistence.journal.leveldb.dir",
        "akka.persistence.journal.leveldb-shared.store.dir",
        "akka.persistence.snapshot-store.local.dir").map(s => new File(sys.settings.config.getString(s)))

    storageLocations.foreach(FileUtils.deleteDirectory)
  }

}
