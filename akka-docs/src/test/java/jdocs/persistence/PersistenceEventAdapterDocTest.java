/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

import akka.persistence.journal.EventAdapter;
import akka.persistence.journal.EventSeq;

public class PersistenceEventAdapterDocTest {

  @SuppressWarnings("unused")
  static
  // #identity-event-adapter
  class MyEventAdapter implements EventAdapter {
    @Override
    public String manifest(Object event) {
      return ""; // if no manifest needed, return ""
    }

    @Override
    public Object toJournal(Object event) {
      return event; // identity
    }

    @Override
    public EventSeq fromJournal(Object event, String manifest) {
      return EventSeq.single(event); // identity
    }
  }
  // #identity-event-adapter
}
