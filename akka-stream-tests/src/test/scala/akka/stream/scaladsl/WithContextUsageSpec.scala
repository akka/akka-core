/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable

import akka.NotUsed
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink

class WithContextUsageSpec extends StreamSpec {

  "Context propagation used for committing offsets" must {

    "be able to commit on offset change" in {
      val testRange = 0 to 10
      val input = genInput(testRange)
      val expectedOffsets = testRange.map(ix => Offset(ix)).init

      val f: (Record => Record) = record => record.copy(value = record.value + 1)
      val expectedRecords = toRecords(input).map(f)

      val src = createSourceWithContext(input).map(f).asSource

      src
        .map { case (e, _) => e }
        .runWith(TestSink[Record]())
        .request(input.size)
        .expectNextN(expectedRecords)
        .expectComplete()

      src
        .map { case (_, ctx) => ctx }
        .toMat(commitOffsets)(Keep.right)
        .run()
        .request(input.size)
        .expectNextN(expectedOffsets)
        .expectComplete()
    }

    "only commit filtered offsets on offset change" in {
      val testRange = 0 to 10
      val input = genInput(testRange)

      val f: (Record => Boolean) = record => record.key.endsWith("2")
      val expectedOffsets = input.filter(cm => f(cm.record)).map(cm => Offset(cm)).init
      val expectedRecords = toRecords(input).filter(f)

      val src = createSourceWithContext(input).filter(f).asSource

      src
        .map { case (e, _) => e }
        .runWith(TestSink[Record]())
        .request(input.size)
        .expectNextN(expectedRecords)
        .expectComplete()

      src
        .map { case (_, ctx) => ctx }
        .toMat(commitOffsets)(Keep.right)
        .run()
        .request(input.size)
        .expectNextN(expectedOffsets)
        .expectComplete()
    }

    "only commit after mapConcat on offset change" in {
      val testRange = 0 to 10
      val input = genInput(testRange)

      val f: (Record => List[Record]) = record => List(record, record, record)
      val expectedOffsets = testRange.map(ix => Offset(ix)).init
      val expectedRecords = toRecords(input).flatMap(f)

      val src = createSourceWithContext(input).mapConcat(f).asSource

      src
        .map { case (e, _) => e }
        .runWith(TestSink[Record]())
        .request(expectedRecords.size)
        .expectNextN(expectedRecords)
        .expectComplete()

      src
        .map { case (_, ctx) => ctx }
        .toMat(commitOffsets)(Keep.right)
        .run()
        .request(input.size)
        .expectNextN(expectedOffsets)
        .expectComplete()
    }

    "commit offsets after grouped on offset change" in {
      val groupSize = 2
      val testRange = 0 to 10
      val input = genInput(testRange)

      val expectedOffsets = testRange.grouped(2).map(ixs => Offset(ixs.last)).toVector.init
      val expectedMultiRecords = toRecords(input).grouped(groupSize).map(l => MultiRecord(l)).toVector

      val src = createSourceWithContext(input).grouped(groupSize).map(l => MultiRecord(l)).mapContext(_.last).asSource

      src
        .map { case (e, _) => e }
        .runWith(TestSink[MultiRecord]())
        .request(expectedMultiRecords.size)
        .expectNextN(expectedMultiRecords)
        .expectComplete()

      src
        .map { case (_, ctx) => ctx }
        .toMat(commitOffsets)(Keep.right)
        .run()
        .request(input.size)
        .expectNextN(expectedOffsets)
        .expectComplete()
    }

    "commit offsets after mapConcat + grouped on offset change" in {
      val groupSize = 2
      val testRange = 0 to 10
      val input = genInput(testRange)

      val f: (Record => List[Record]) = record => List(record, record, record)

      // the mapConcat creates bigger lists than the groups, which is why all offsets are seen.
      // (The mapContext selects the last offset in a group)
      val expectedOffsets = testRange.map(ix => Offset(ix)).init
      val expectedMultiRecords = toRecords(input).flatMap(f).grouped(groupSize).map(l => MultiRecord(l)).toVector

      val src = createSourceWithContext(input)
        .mapConcat(f)
        .grouped(groupSize)
        .map(l => MultiRecord(l))
        .mapContext(_.last)
        .asSource

      src
        .map { case (e, _) => e }
        .runWith(TestSink[MultiRecord]())
        .request(expectedMultiRecords.size)
        .expectNextN(expectedMultiRecords)
        .expectComplete()

      src
        .map { case (_, ctx) => ctx }
        .toMat(commitOffsets)(Keep.right)
        .run()
        .request(input.size)
        .expectNextN(expectedOffsets)
        .expectComplete()
    }

    def genInput(range: Range) =
      range
        .map(ix => Consumer.CommittableMessage(Record(genKey(ix), genValue(ix)), Consumer.CommittableOffsetImpl(ix)))
        .toVector
    def toRecords(committableMessages: Vector[Consumer.CommittableMessage[Record]]) = committableMessages.map(_.record)
    def genKey(ix: Int) = s"k$ix"
    def genValue(ix: Int) = s"v$ix"
  }

  def createSourceWithContext(
      committableMessages: Vector[Consumer.CommittableMessage[Record]]): SourceWithContext[Record, Offset, NotUsed] =
    Consumer
      .committableSource(committableMessages)
      .asSourceWithContext(m => Offset(m.committableOffset.offset))
      .map(_.record)

  def commitOffsets = commit[Offset](Offset.Uninitialized)
  def commit[Ctx](uninitialized: Ctx): Sink[Ctx, Probe[Ctx]] = {
    val testSink = TestSink[Ctx]()
    Flow[Ctx]
      .statefulMapConcat { () =>
        {
          var prevCtx: Ctx = uninitialized
          ctx => {
            val res =
              if (prevCtx != uninitialized && ctx != prevCtx) Vector(prevCtx)
              else Vector.empty[Ctx]

            prevCtx = ctx
            res
          }
        }
      }
      .toMat(testSink)(Keep.right)
  }
}

object Offset {
  val Uninitialized = Offset(-1)
  def apply(cm: Consumer.CommittableMessage[Record]): Offset = Offset(cm.committableOffset.offset)
}

case class Offset(value: Int)

case class Record(key: String, value: String)
case class Committed[R](record: R, offset: Int)
case class MultiRecord(records: immutable.Seq[Record])

object Consumer {
  def committableSource(
      committableMessages: Vector[CommittableMessage[Record]]): Source[CommittableMessage[Record], NotUsed] = {
    Source(committableMessages)
  }
  case class CommittableMessage[V](record: V, committableOffset: CommittableOffset)

  trait Committable {
    def commit(): Unit
  }

  trait CommittableOffset extends Committable {
    def offset: Int
  }

  case class CommittableOffsetImpl(offset: Int) extends CommittableOffset {
    def commit(): Unit = {}
  }
}
