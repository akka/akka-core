/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl._
import akka.util.ByteString

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@Fork(1)
@Threads(1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, batchSize = 1)
class FileSourcesScaleBenchmark {

  /**
   * Benchmark                               (bufSize)  Mode  Cnt  Score   Error  Units
   * FileSourcesScaleBenchmark.flatMapMerge       2048  avgt   10  1.587 ± 0.118   s/op
   * FileSourcesScaleBenchmark.mapAsync           2048  avgt   10  0.899 ± 0.103   s/op
   */
  implicit val system: ActorSystem = ActorSystem("file-sources-benchmark")

  val FILES_NUMBER = 40
  val files: Seq[Path] = {
    val line = ByteString("x" * 2048 + "\n")
    (1 to FILES_NUMBER).map(i => {
      val f = Files.createTempFile(getClass.getName, s"$i.bench.tmp")

      val ft = Source
        .fromIterator(() => Iterator.continually(line))
        .take(20000) // adjust as needed
        .runWith(FileIO.toPath(f))
      Await.result(ft, 300.seconds)
      f
    })
  }

  @Param(Array("2048"))
  var bufSize = 0

  var fileChannelSource: Seq[Source[ByteString, Future[IOResult]]] = _

  @Setup
  def setup(): Unit = {
    fileChannelSource = files.map(FileIO.fromPath(_, bufSize))
  }

  @TearDown
  def teardown(): Unit = {
    files.foreach(Files.delete)
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
  }

  @Benchmark
  def flatMapMerge(): Unit = {
    val h = Source
      .fromIterator(() => files.iterator)
      .flatMapMerge(FILES_NUMBER, path => FileIO.fromPath(path, bufSize))
      .runWith(Sink.ignore)

    Await.result(h, 300.seconds)
  }

  @Benchmark
  def mapAsync(): Unit = {
    val h = Source
      .fromIterator(() => files.iterator)
      .mapAsync(FILES_NUMBER)(path => FileIO.fromPath(path, bufSize).runWith(Sink.ignore))
      .runWith(Sink.ignore)

    Await.result(h, 300.seconds)
  }

}
