/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.annotation.InternalApi
import akka.cluster.ddata.Key.KeyId
import akka.event.LoggingAdapter

/**
 * INTERNAL API
 *
 * This class is not thread-safe. It is supposed to be used from an actor.
 */
@InternalApi private[akka] class PayloadSizeAggregator(
    log: LoggingAdapter,
    logSizeExceeding: Int,
    val maxFrameSize: Int) {
  private val warnSizeExceeding = maxFrameSize * 3 / 4
  private var maxPayloadBytes: Map[String, Int] = Map.empty

  def updatePayloadSize(key: KeyId, size: Int): Unit = {
    if (size > 0) { // deleted has size 0
      // 10% threshold until next log
      def newMax = (size * 1.1).toInt

      def logSize(): Unit = {
        if (size >= warnSizeExceeding)
          log.warning(
            "Distributed data size for [{}] is [{}] bytes. Close to max remote message payload size.",
            key,
            size)
        else
          log.info("Distributed data size for [{}] is [{}] bytes.", key, size)
      }

      maxPayloadBytes.get(key) match {
        case Some(max) =>
          if (size > max) {
            maxPayloadBytes = maxPayloadBytes.updated(key, newMax)
            if (size >= logSizeExceeding)
              logSize()
          }
        case None =>
          maxPayloadBytes = maxPayloadBytes.updated(key, newMax)
          if (size >= logSizeExceeding)
            logSize()
      }
    }
  }

  def getMaxSize(key: KeyId): Int = {
    maxPayloadBytes.getOrElse(key, 0)
  }

  def remove(key: KeyId): Unit =
    maxPayloadBytes -= key

}
