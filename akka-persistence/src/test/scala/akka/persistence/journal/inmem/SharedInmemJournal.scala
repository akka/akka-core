/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.inmem

import scala.collection.immutable

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.persistence.PersistentRepr
import akka.util.OptionVal

object SharedInmemJournal {
  val PluginId = "akka.persistence.journal.shared-inmem"

  val config: Config = ConfigFactory.parseString(s"""
    $PluginId {
      class = "${classOf[SharedInmemJournal].getName}"
      plugin-dispatcher = "akka.actor.default-dispatcher"
    }
    """)

  // shared by journal actors of several actor systems, so access is synchronized
  private val storage = new InmemMessages {}
}

/**
 * In-memory journal with storage shared by all actor systems in the JVM. For tests that need
 * events to survive an actor system restart. Tests must use distinct persistence ids.
 */
class SharedInmemJournal(cfg: Config) extends InmemJournal(cfg) {
  import SharedInmemJournal.storage

  override def add(p: PersistentRepr): Unit = storage.synchronized(storage.add(p))

  override def delete(pid: String, snr: Long): Unit = storage.synchronized(storage.delete(pid, snr))

  override def read(
      pid: String,
      fromSnr: Long,
      toSnr: Long,
      max: Long): immutable.Seq[(PersistentRepr, OptionVal[Any])] =
    storage.synchronized(storage.read(pid, fromSnr, toSnr, max))

  override def highestSequenceNr(pid: String): Long = storage.synchronized(storage.highestSequenceNr(pid))
}
