/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence.query;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.persistence.journal.Tagged;
import akka.persistence.journal.WriteEventAdapter;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.Sequence;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.javadsl.Source;
import java.util.HashSet;
import java.util.Set;

public class LeveldbPersistenceQueryDocTest {

  final ActorSystem system = ActorSystem.create();

  public void demonstrateReadJournal() {
    // #get-read-journal
    LeveldbReadJournal queries =
        PersistenceQuery.get(system)
            .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());
    // #get-read-journal
  }

  public void demonstrateEventsByPersistenceId() {
    // #EventsByPersistenceId
    LeveldbReadJournal queries =
        PersistenceQuery.get(system)
            .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());

    Source<EventEnvelope, NotUsed> source =
        queries.eventsByPersistenceId("some-persistence-id", 0, Long.MAX_VALUE);
    // #EventsByPersistenceId
  }

  public void demonstrateAllPersistenceIds() {
    // #AllPersistenceIds
    LeveldbReadJournal queries =
        PersistenceQuery.get(system)
            .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());

    Source<String, NotUsed> source = queries.persistenceIds();
    // #AllPersistenceIds
  }

  public void demonstrateEventsByTag() {
    // #EventsByTag
    LeveldbReadJournal queries =
        PersistenceQuery.get(system)
            .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());

    Source<EventEnvelope, NotUsed> source = queries.eventsByTag("green", new Sequence(0L));
    // #EventsByTag
  }

  public
  // #tagger
  static class MyTaggingEventAdapter implements WriteEventAdapter {

    @Override
    public Object toJournal(Object event) {
      if (event instanceof String) {
        String s = (String) event;
        Set<String> tags = new HashSet<String>();
        if (s.contains("green")) tags.add("green");
        if (s.contains("black")) tags.add("black");
        if (s.contains("blue")) tags.add("blue");
        if (tags.isEmpty()) return event;
        else return new Tagged(event, tags);
      } else {
        return event;
      }
    }

    @Override
    public String manifest(Object event) {
      return "";
    }
  }
  // #tagger
}
