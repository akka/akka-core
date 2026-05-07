/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.query

import akka.NotUsed
import akka.actor._
import akka.persistence.query._
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.javadsl
import akka.testkit.AkkaSpec
import akka.util.Timeout
import org.reactivestreams.Subscriber

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import com.typesafe.config.Config

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
object PersistenceQueryDocSpec {

  implicit val timeout: Timeout = Timeout(3.seconds)

  //#advanced-journal-query-types
  final case class RichEvent(tags: Set[String], payload: Any)

  // a plugin can provide:
  case class QueryMetadata(deterministicOrder: Boolean, infinite: Boolean)
  //#advanced-journal-query-types

  //#my-read-journal
  class MyReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

    private val readJournal: MyScaladslReadJournal =
      new MyScaladslReadJournal(system, config)

    override def scaladslReadJournal(): MyScaladslReadJournal =
      readJournal

    override def javadslReadJournal(): MyJavadslReadJournal =
      new MyJavadslReadJournal(readJournal)
  }

  class MyScaladslReadJournal(system: ExtendedActorSystem, config: Config)
      extends akka.persistence.query.scaladsl.ReadJournal
      with akka.persistence.query.scaladsl.EventsByTagQuery
      with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
      with akka.persistence.query.scaladsl.PersistenceIdsQuery
      with akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery {

    private val refreshInterval: FiniteDuration =
      config.getDuration("refresh-interval", MILLISECONDS).millis

    /**
     * You can use `NoOffset` to retrieve all events with a given tag or retrieve a subset of all
     * events by specifying a `Sequence` `offset`. The `offset` corresponds to an ordered sequence number for
     * the specific tag. Note that the corresponding offset of each event is provided in the
     * [[akka.persistence.query.EventEnvelope]], which makes it possible to resume the
     * stream at a later point from a given offset.
     *
     * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
     * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
     * as the `offset` parameter in a subsequent query.
     */
    override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = offset match {
      case Sequence(offsetValue) =>
        Source.fromGraph(new MyEventsByTagSource(tag, offsetValue, refreshInterval))
      case NoOffset => eventsByTag(tag, Sequence(0L)) //recursive
      case _ =>
        throw new IllegalArgumentException("MyJournal does not support " + offset.getClass.getName + " offsets")
    }

    override def eventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
      // implement in a similar way as eventsByTag
      ???
    }

    override def persistenceIds(): Source[String, NotUsed] = {
      // implement in a similar way as eventsByTag
      ???
    }

    override def currentPersistenceIds(): Source[String, NotUsed] = {
      // implement in a similar way as eventsByTag
      ???
    }

    // possibility to add more plugin specific queries

    //#advanced-journal-query-definition
    def byTagsWithMeta(tags: Set[String]): Source[RichEvent, QueryMetadata] = {
      //#advanced-journal-query-definition
      // implement in a similar way as eventsByTag
      ???
    }

  }

  class MyJavadslReadJournal(scaladslReadJournal: MyScaladslReadJournal)
      extends akka.persistence.query.javadsl.ReadJournal
      with akka.persistence.query.javadsl.EventsByTagQuery
      with akka.persistence.query.javadsl.EventsByPersistenceIdQuery
      with akka.persistence.query.javadsl.PersistenceIdsQuery
      with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery {

    override def eventsByTag(tag: String, offset: Offset = Sequence(0L)): javadsl.Source[EventEnvelope, NotUsed] =
      scaladslReadJournal.eventsByTag(tag, offset).asJava

    override def eventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long = 0L,
        toSequenceNr: Long = Long.MaxValue): javadsl.Source[EventEnvelope, NotUsed] =
      scaladslReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

    override def persistenceIds(): javadsl.Source[String, NotUsed] =
      scaladslReadJournal.persistenceIds().asJava

    override def currentPersistenceIds(): javadsl.Source[String, NotUsed] =
      scaladslReadJournal.currentPersistenceIds().asJava

    // possibility to add more plugin specific queries

    def byTagsWithMeta(tags: java.util.Set[String]): javadsl.Source[RichEvent, QueryMetadata] = {
      import scala.jdk.CollectionConverters._
      scaladslReadJournal.byTagsWithMeta(tags.asScala.toSet).asJava
    }
  }

  //#my-read-journal

  case class ComplexState() {
    def readyToSave = false
  }
  case class Record(any: Any)
  class ExampleStore { def save(record: Record) = Future.successful(42L) }

  val JournalId = "akka.persistence.query.my-read-journal"

  class X {

    def convertToReadSideTypes(in: Any): Any = ???

    object ReactiveStreamsCompatibleDBDriver {
      def batchWriter: Subscriber[immutable.Seq[Any]] = ???
    }

    //#projection-into-different-store-rs
    implicit val system: ActorSystem = ActorSystem()

    val readJournal =
      PersistenceQuery(system).readJournalFor[MyScaladslReadJournal](JournalId)
    val dbBatchWriter: Subscriber[immutable.Seq[Any]] =
      ReactiveStreamsCompatibleDBDriver.batchWriter

    // Using an example (Reactive Streams) Database driver
    readJournal
      .eventsByPersistenceId("user-1337", fromSequenceNr = 0L, toSequenceNr = Long.MaxValue)
      .map(envelope => envelope.event)
      .map(convertToReadSideTypes) // convert to datatype
      .grouped(20) // batch inserts into groups of 20
      .runWith(Sink.fromSubscriber(dbBatchWriter)) // write batches to read-side database
    //#projection-into-different-store-rs
  }

}

@nowarn("msg=never used") // sample snippets
class PersistenceQueryDocSpec(s: String) extends AkkaSpec(s) {
  import PersistenceQueryDocSpec._

  def this() =
    this("""
        akka.persistence.query.my-read-journal {
          class = "docs.persistence.query.PersistenceQueryDocSpec$MyReadJournalProvider"
          refresh-interval = 3s
        }
      """)

  class BasicUsage {
    //#basic-usage
    // obtain read journal by plugin id
    val readJournal =
      PersistenceQuery(system).readJournalFor[MyScaladslReadJournal]("akka.persistence.query.my-read-journal")

    // issue query to journal
    val source: Source[EventEnvelope, NotUsed] =
      readJournal.eventsByPersistenceId("user-1337", 0, Long.MaxValue)

    // materialize stream, consuming events
    source.runForeach { event =>
      println("Event: " + event)
    }
    //#basic-usage

    //#all-persistence-ids-live
    readJournal.persistenceIds()
    //#all-persistence-ids-live

    //#all-persistence-ids-snap
    readJournal.currentPersistenceIds()
    //#all-persistence-ids-snap

    trait OrderCompleted

    //#events-by-tag
    // assuming journal is able to work with numeric offsets we can:

    val completedOrders: Source[EventEnvelope, NotUsed] =
      readJournal.eventsByTag("order-completed", Offset.noOffset)

    // find first 10 completed orders:
    val firstCompleted: Future[Vector[OrderCompleted]] =
      completedOrders
        .map(_.event)
        .collectType[OrderCompleted]
        .take(10) // cancels the query stream after pulling 10 elements
        .runFold(Vector.empty[OrderCompleted])(_ :+ _)

    // start another query, from the known offset
    val furtherOrders = readJournal.eventsByTag("order-completed", offset = Sequence(10))
    //#events-by-tag

    //#events-by-persistent-id
    readJournal.eventsByPersistenceId("user-us-1337", fromSequenceNr = 0L, toSequenceNr = Long.MaxValue)

    //#events-by-persistent-id

    //#advanced-journal-query-usage
    val query: Source[RichEvent, QueryMetadata] =
      readJournal.byTagsWithMeta(Set("red", "blue"))

    query
      .mapMaterializedValue { meta =>
        println(
          s"The query is: " +
          s"ordered deterministically: ${meta.deterministicOrder}, " +
          s"infinite: ${meta.infinite}")
      }
      .map { event =>
        println(s"Event payload: ${event.payload}")
      }
      .runWith(Sink.ignore)

    //#advanced-journal-query-usage
  }

  //#projection-into-different-store
  class MyResumableProjection(name: String) {
    def saveProgress(offset: Offset): Future[Long] = ???
    def latestOffset: Future[Long] = ???
  }
  //#projection-into-different-store

  class RunWithAsyncFunction {
    val readJournal =
      PersistenceQuery(system).readJournalFor[MyScaladslReadJournal]("akka.persistence.query.my-read-journal")

    //#projection-into-different-store-simple-classes
    trait ExampleStore {
      def save(event: Any): Future[Unit]
    }
    //#projection-into-different-store-simple-classes

    //#projection-into-different-store-simple
    val store: ExampleStore = ???

    readJournal
      .eventsByTag("bid", NoOffset)
      .mapAsync(1) { e =>
        store.save(e)
      }
      .runWith(Sink.ignore)
    //#projection-into-different-store-simple
  }

}
