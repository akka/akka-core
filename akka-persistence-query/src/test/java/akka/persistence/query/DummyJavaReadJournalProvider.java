/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DummyJavaReadJournalProvider implements ReadJournalProvider {

  public static final Config config =
      ConfigFactory.parseString(
          DummyJavaReadJournal.Identifier
              + " { \n"
              + "   class = \""
              + DummyJavaReadJournalProvider.class.getCanonicalName()
              + "\" \n"
              + " }\n\n");

  private final DummyJavaReadJournal readJournal = new DummyJavaReadJournal();

  @Override
  public DummyJavaReadJournalForScala scaladslReadJournal() {
    return new DummyJavaReadJournalForScala(readJournal);
  }

  @Override
  public DummyJavaReadJournal javadslReadJournal() {
    return readJournal;
  }
}
