/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb

import java.util

import akka.annotation.InternalApi
import akka.stream.Outlet
import akka.stream.stage.GraphStageLogic
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[leveldb] abstract trait Buffer[T] { self: GraphStageLogic =>

  def doPush(out: Outlet[T], elem: T): Unit

  private val buf: java.util.LinkedList[T] = new util.LinkedList[T]()
  def buffer(element: T): Unit = {
    buf.add(element)
  }
  def buffer(all: Set[T]): Unit = {
    buf.addAll(all.asJava)
  }
  def deliverBuf(out: Outlet[T]): Unit = {
    if (!buf.isEmpty && isAvailable(out)) {
      doPush(out, buf.remove())
    }
  }
  def bufferSize: Int = buf.size
  def bufferEmpty: Boolean = buf.isEmpty
}
