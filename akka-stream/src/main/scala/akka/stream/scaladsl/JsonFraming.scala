/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NonFatal

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.impl.JsonObjectParser
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage.{ GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString

/** Provides JSON framing operators that can separate valid JSON objects from incoming [[ByteString]] objects. */
object JsonFraming {

  /** Thrown if upstream completes with a partial object in the buffer. */
  class PartialObjectException(msg: String = "JSON stream completed with partial content in the buffer!")
      extends FramingException(msg)

  /**
   * Returns a Flow that implements a "brace counting" based framing operator for emitting valid JSON chunks.
   * It scans the incoming data stream for valid JSON objects and returns chunks of ByteStrings containing only those valid chunks.
   *
   * Typical examples of data that one may want to frame using this operator include:
   *
   * **Very large arrays**:
   * {{{
   *   [{"id": 1}, {"id": 2}, [...], {"id": 999}]
   * }}}
   *
   * **Multiple concatenated JSON objects** (with, or without commas between them):
   *
   * {{{
   *   {"id": 1}, {"id": 2}, [...], {"id": 999}
   * }}}
   *
   * The framing works independently of formatting, i.e. it will still emit valid JSON elements even if two
   * elements are separated by multiple newlines or other whitespace characters. And of course is insensitive
   * (and does not impact the emitting frame) to the JSON object's internal formatting.
   *
   * If the stream completes while mid-object, the stage will fail with a [[PartialObjectException]].
   *
   * @param maximumObjectLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                            this Flow will fail the stream.
   */
  def objectScanner(maximumObjectLength: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new SimpleLinearGraphStage[ByteString] {

      override protected def initialAttributes: Attributes = Attributes.name("JsonFraming.objectScanner")

      override def createLogic(inheritedAttributes: Attributes) =
        new GraphStageLogic(shape) with InHandler with OutHandler {
          private val buffer = new JsonObjectParser(maximumObjectLength)

          setHandlers(in, out, this)

          override def onPush(): Unit = {
            buffer.offer(grab(in))
            tryPopBuffer()
          }

          override def onPull(): Unit =
            tryPopBuffer()

          override def onUpstreamFinish(): Unit = {
            buffer.poll() match {
              case Some(json) => emit(out, json)
              case _          => complete()
            }
          }

          def tryPopBuffer(): Unit = {
            try buffer.poll() match {
              case Some(json) => push(out, json)
              case _          => if (isClosed(in)) complete() else pull(in)
            } catch {
              case NonFatal(ex) => failStage(ex)
            }
          }

          def complete(): Unit =
            if (buffer.canComplete) completeStage()
            else failStage(new PartialObjectException)
        }
    })

}
