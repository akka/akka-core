/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.japi

import com.typesafe.config.ConfigFactory
import org.scalatest.DoNotDiscover

import akka.persistence.japi.journal.JavaJournalSpec

/* Only checking that compilation works with the constructor here as expected (no other abstract fields leaked) */
@DoNotDiscover
class JavaJournalSpecSpec extends JavaJournalSpec(ConfigFactory.parseString(""))
