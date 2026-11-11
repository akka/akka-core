/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.inmem

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

import com.typesafe.config.Config

import akka.persistence.AtomicWrite

object FailingInmemJournal {
  // the target journal runs in one JVM, so the test toggles it there
  val failWrites = new AtomicBoolean(false)
}

/**
 * Inmem journal that fails writes while `FailingInmemJournal.failWrites` is set. In contrast to blackholing
 * the network, a write is then never lost without a reply.
 */
class FailingInmemJournal(cfg: Config) extends InmemJournal(cfg) {
  def this() = this(com.typesafe.config.ConfigFactory.empty())

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    if (FailingInmemJournal.failWrites.get) Future.failed(new RuntimeException("Simulated journal failure"))
    else super.asyncWriteMessages(messages)
}
