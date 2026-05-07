/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query;

import akka.NotUsed;
import akka.persistence.query.javadsl.PersistenceIdsQuery;
import akka.persistence.query.javadsl.ReadJournal;
import akka.stream.javadsl.Source;
import java.util.Iterator;

/** Use for tests only! Emits infinite stream of strings (representing queried for events). */
public class DummyJavaReadJournal implements ReadJournal, PersistenceIdsQuery {
  public static final String Identifier = "akka.persistence.query.journal.dummy-java";

  @Override
  public Source<String, NotUsed> persistenceIds() {
    return Source.fromIterator(
        () ->
            new Iterator<String>() {
              private int i = 0;

              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public String next() {
                return "" + (i++);
              }
            });
  }
}
