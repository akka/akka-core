/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

import akka.NotUsed;
import akka.actor.*;
import akka.persistence.query.*;
import akka.persistence.query.Offset;
import akka.persistence.query.Sequence;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import jdocs.persistence.query.MyEventsByTagSource;
import org.reactivestreams.Subscriber;

public class PersistenceQueryDocTest {

  final ActorSystem system = ActorSystem.create();

  public
  // #advanced-journal-query-types
  static class RichEvent {
    public final Set<String> tags;
    public final Object payload;

    public RichEvent(Set<String> tags, Object payload) {
      this.tags = tags;
      this.payload = payload;
    }
  }
  // #advanced-journal-query-types

  public
  // #advanced-journal-query-types
  // a plugin can provide:
  static final class QueryMetadata {
    public final boolean deterministicOrder;
    public final boolean infinite;

    public QueryMetadata(boolean deterministicOrder, boolean infinite) {
      this.deterministicOrder = deterministicOrder;
      this.infinite = infinite;
    }
  }
  // #advanced-journal-query-types

  interface OrderCompleted {}

  public
  // #my-read-journal
  static class MyReadJournalProvider implements ReadJournalProvider {
    private final MyJavadslReadJournal javadslReadJournal;

    public MyReadJournalProvider(ExtendedActorSystem system, Config config) {
      this.javadslReadJournal = new MyJavadslReadJournal(system, config);
    }

    @Override
    public MyScaladslReadJournal scaladslReadJournal() {
      return new MyScaladslReadJournal(javadslReadJournal);
    }

    @Override
    public MyJavadslReadJournal javadslReadJournal() {
      return this.javadslReadJournal;
    }
  }
  // #my-read-journal

  public
  // #my-read-journal
  static class MyJavadslReadJournal
      implements akka.persistence.query.javadsl.ReadJournal,
          akka.persistence.query.javadsl.EventsByTagQuery,
          akka.persistence.query.javadsl.EventsByPersistenceIdQuery,
          akka.persistence.query.javadsl.PersistenceIdsQuery,
          akka.persistence.query.javadsl.CurrentPersistenceIdsQuery {

    private final Duration refreshInterval;
    private Connection conn;

    public MyJavadslReadJournal(ExtendedActorSystem system, Config config) {
      refreshInterval = config.getDuration("refresh-interval");
    }

    /**
     * You can use `NoOffset` to retrieve all events with a given tag or retrieve a subset of all
     * events by specifying a `Sequence` `offset`. The `offset` corresponds to an ordered sequence
     * number for the specific tag. Note that the corresponding offset of each event is provided in
     * the [[akka.persistence.query.EventEnvelope]], which makes it possible to resume the stream at
     * a later point from a given offset.
     *
     * <p>The `offset` is exclusive, i.e. the event with the exact same sequence number will not be
     * included in the returned stream. This means that you can use the offset that is returned in
     * `EventEnvelope` as the `offset` parameter in a subsequent query.
     */
    @Override
    public Source<EventEnvelope, NotUsed> eventsByTag(String tag, Offset offset) {
      if (offset instanceof Sequence) {
        Sequence sequenceOffset = (Sequence) offset;
        return Source.fromGraph(
            new MyEventsByTagSource(conn, tag, sequenceOffset.value(), refreshInterval));
      } else if (offset == NoOffset.getInstance())
        return eventsByTag(tag, Offset.sequence(0L)); // recursive
      else
        throw new IllegalArgumentException(
            "MyJavadslReadJournal does not support " + offset.getClass().getName() + " offsets");
    }

    @Override
    public Source<EventEnvelope, NotUsed> eventsByPersistenceId(
        String persistenceId, long fromSequenceNr, long toSequenceNr) {
      // implement in a similar way as eventsByTag
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Source<String, NotUsed> persistenceIds() {
      // implement in a similar way as eventsByTag
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Source<String, NotUsed> currentPersistenceIds() {
      // implement in a similar way as eventsByTag
      throw new UnsupportedOperationException("Not implemented yet");
    }

    // possibility to add more plugin specific queries

    // #advanced-journal-query-definition
    public Source<RichEvent, QueryMetadata> byTagsWithMeta(Set<String> tags) {
      // #advanced-journal-query-definition
      // implement in a similar way as eventsByTag
      throw new UnsupportedOperationException("Not implemented yet");
    }
  }
  // #my-read-journal

  public
  // #my-read-journal
  static class MyScaladslReadJournal
      implements akka.persistence.query.scaladsl.ReadJournal,
          akka.persistence.query.scaladsl.EventsByTagQuery,
          akka.persistence.query.scaladsl.EventsByPersistenceIdQuery,
          akka.persistence.query.scaladsl.PersistenceIdsQuery,
          akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery {

    private final MyJavadslReadJournal javadslReadJournal;

    public MyScaladslReadJournal(MyJavadslReadJournal javadslReadJournal) {
      this.javadslReadJournal = javadslReadJournal;
    }

    @Override
    public akka.stream.scaladsl.Source<EventEnvelope, NotUsed> eventsByTag(
        String tag, akka.persistence.query.Offset offset) {
      return javadslReadJournal.eventsByTag(tag, offset).asScala();
    }

    @Override
    public akka.stream.scaladsl.Source<EventEnvelope, NotUsed> eventsByPersistenceId(
        String persistenceId, long fromSequenceNr, long toSequenceNr) {
      return javadslReadJournal
          .eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
          .asScala();
    }

    @Override
    public akka.stream.scaladsl.Source<String, NotUsed> persistenceIds() {
      return javadslReadJournal.persistenceIds().asScala();
    }

    @Override
    public akka.stream.scaladsl.Source<String, NotUsed> currentPersistenceIds() {
      return javadslReadJournal.currentPersistenceIds().asScala();
    }

    // possibility to add more plugin specific queries

    public akka.stream.scaladsl.Source<RichEvent, QueryMetadata> byTagsWithMeta(
        scala.collection.Set<String> tags) {
      Set<String> jTags = scala.jdk.javaapi.CollectionConverters.asJava(tags);
      return javadslReadJournal.byTagsWithMeta(jTags).asScala();
    }
  }
  // #my-read-journal

  void demonstrateBasicUsage() {
    final ActorSystem system = ActorSystem.create();

    // #basic-usage
    // obtain read journal by plugin id
    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // issue query to journal
    Source<EventEnvelope, NotUsed> source =
        readJournal.eventsByPersistenceId("user-1337", 0, Long.MAX_VALUE);

    // materialize stream, consuming events
    source.runForeach(event -> System.out.println("Event: " + event), system);
    // #basic-usage
  }

  void demonstrateAllPersistenceIdsLive() {
    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #all-persistence-ids-live
    readJournal.persistenceIds();
    // #all-persistence-ids-live
  }

  void demonstrateNoRefresh() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #all-persistence-ids-snap
    readJournal.currentPersistenceIds();
    // #all-persistence-ids-snap
  }

  void demonstrateRefresh() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #events-by-persistent-id
    readJournal.eventsByPersistenceId("user-us-1337", 0L, Long.MAX_VALUE);
    // #events-by-persistent-id
  }

  void demonstrateEventsByTag() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #events-by-tag
    // assuming journal is able to work with numeric offsets we can:
    final Source<EventEnvelope, NotUsed> completedOrders =
        readJournal.eventsByTag("order-completed", new Sequence(0L));

    // find first 10 completed orders:
    final CompletionStage<List<OrderCompleted>> firstCompleted =
        completedOrders
            .map(EventEnvelope::event)
            .collectType(OrderCompleted.class)
            .take(10) // cancels the query stream after pulling 10 elements
            .runFold(
                new ArrayList<>(10),
                (acc, e) -> {
                  acc.add(e);
                  return acc;
                },
                system);

    // start another query, from the known offset
    Source<EventEnvelope, NotUsed> furtherOrders =
        readJournal.eventsByTag("order-completed", new Sequence(10));
    // #events-by-tag
  }

  void demonstrateMaterializedQueryValues() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #advanced-journal-query-usage

    Set<String> tags = new HashSet<String>();
    tags.add("red");
    tags.add("blue");
    final Source<RichEvent, QueryMetadata> events =
        readJournal
            .byTagsWithMeta(tags)
            .mapMaterializedValue(
                meta -> {
                  System.out.println(
                      "The query is: "
                          + "ordered deterministically: "
                          + meta.deterministicOrder
                          + " "
                          + "infinite: "
                          + meta.infinite);
                  return meta;
                });

    events
        .map(
            event -> {
              System.out.println("Event payload: " + event.payload);
              return event.payload;
            })
        .runWith(Sink.ignore(), system);

    // #advanced-journal-query-usage
  }

  class ReactiveStreamsCompatibleDBDriver {
    Subscriber<List<Object>> batchWriter() {
      return null;
    }
  }

  void demonstrateWritingIntoDifferentStore() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #projection-into-different-store-rs
    final ReactiveStreamsCompatibleDBDriver driver = new ReactiveStreamsCompatibleDBDriver();
    final Subscriber<List<Object>> dbBatchWriter = driver.batchWriter();

    // Using an example (Reactive Streams) Database driver
    readJournal
        .eventsByPersistenceId("user-1337", 0L, Long.MAX_VALUE)
        .map(envelope -> envelope.event())
        .grouped(20) // batch inserts into groups of 20
        .runWith(Sink.fromSubscriber(dbBatchWriter), system); // write batches to read-side database
    // #projection-into-different-store-rs
  }

  // #projection-into-different-store-simple-classes
  static class ExampleStore {
    CompletionStage<Void> save(Object any) {
      // ...
      // #projection-into-different-store-simple-classes
      return null;
      // #projection-into-different-store-simple-classes
    }
  }
  // #projection-into-different-store-simple-classes

  void demonstrateWritingIntoDifferentStoreWithMapAsync() {
    final ActorSystem system = ActorSystem.create();

    final MyJavadslReadJournal readJournal =
        PersistenceQuery.get(system)
            .getReadJournalFor(
                MyJavadslReadJournal.class, "akka.persistence.query.my-read-journal");

    // #projection-into-different-store-simple
    final ExampleStore store = new ExampleStore();

    readJournal
        .eventsByTag("bid", new Sequence(0L))
        .mapAsync(1, store::save)
        .runWith(Sink.ignore(), system);
    // #projection-into-different-store-simple
  }

  // #projection-into-different-store
  static class MyResumableProjection {
    private final String name;

    public MyResumableProjection(String name) {
      this.name = name;
    }

    public CompletionStage<Long> saveProgress(Offset offset) {
      // ...
      // #projection-into-different-store
      return null;
      // #projection-into-different-store
    }

    public CompletionStage<Long> latestOffset() {
      // ...
      // #projection-into-different-store
      return null;
      // #projection-into-different-store
    }
  }
  // #projection-into-different-store

  static class ComplexState {

    boolean readyToSave() {
      return false;
    }
  }

  static class Record {
    static Record of(Object any) {
      return new Record();
    }
  }
}
