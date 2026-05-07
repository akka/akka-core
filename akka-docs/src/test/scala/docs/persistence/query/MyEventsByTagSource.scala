/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.query

import akka.persistence.query.{ EventEnvelope, Offset }
import akka.serialization.SerializationExtension
import akka.stream.{ ActorAttributes, Attributes, Outlet, SourceShape }
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic }

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

//#events-by-tag-publisher
class MyEventsByTagSource(tag: String, offset: Long, refreshInterval: FiniteDuration)
    extends GraphStage[SourceShape[EventEnvelope]] {

  private case object Continue
  val out: Outlet[EventEnvelope] = Outlet("MyEventByTagSource.out")
  override def shape: SourceShape[EventEnvelope] = SourceShape(out)

  override protected def initialAttributes: Attributes = Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {
      lazy val system = materializer.system
      private val Limit = 1000
      private val connection: java.sql.Connection = ???
      private var currentOffset = offset
      private var buf = Vector.empty[EventEnvelope]
      private val serialization = SerializationExtension(system)

      override def preStart(): Unit = {
        scheduleWithFixedDelay(Continue, refreshInterval, refreshInterval)
      }

      override def onPull(): Unit = {
        query()
        tryPush()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        // close connection if responsible for doing so
      }

      private def query(): Unit = {
        if (buf.isEmpty) {
          try {
            buf = Select.run(tag, currentOffset, Limit)
          } catch {
            case NonFatal(e) =>
              failStage(e)
          }
        }
      }

      private def tryPush(): Unit = {
        if (buf.nonEmpty && isAvailable(out)) {
          push(out, buf.head)
          buf = buf.tail
        }
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case Continue =>
          query()
          tryPush()
      }

      object Select {
        private def statement() =
          connection.prepareStatement("""
            SELECT id, persistence_id, seq_nr, serializer_id, serializer_manifest, payload 
            FROM journal WHERE tag = ? AND id > ? 
            ORDER BY id LIMIT ?
      """)

        def run(tag: String, from: Long, limit: Int): Vector[EventEnvelope] = {
          val s = statement()
          try {
            s.setString(1, tag)
            s.setLong(2, from)
            s.setLong(3, limit)
            val rs = s.executeQuery()

            val b = Vector.newBuilder[EventEnvelope]
            while (rs.next()) {
              val deserialized = serialization
                .deserialize(rs.getBytes("payload"), rs.getInt("serializer_id"), rs.getString("serializer_manifest"))
                .get
              currentOffset = rs.getLong("id")
              b += EventEnvelope(
                Offset.sequence(currentOffset),
                rs.getString("persistence_id"),
                rs.getLong("seq_nr"),
                deserialized,
                System.currentTimeMillis())
            }
            b.result()
          } finally s.close()
        }
      }
    }

}
//#events-by-tag-publisher
