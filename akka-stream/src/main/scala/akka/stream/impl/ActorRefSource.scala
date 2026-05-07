/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.OverflowStrategies._
import akka.stream.stage._
import akka.util.OptionVal

private object ActorRefSource {
  private sealed trait ActorRefStage { def ref: ActorRef }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class ActorRefSource[T](
    maxBuffer: Int,
    overflowStrategy: OverflowStrategy,
    completionMatcher: PartialFunction[Any, CompletionStrategy],
    failureMatcher: PartialFunction[Any, Throwable])
    extends GraphStageWithMaterializedValue[SourceShape[T], ActorRef] {
  import ActorRefSource._

  val out: Outlet[T] = Outlet[T]("actorRefSource.out")

  override val shape: SourceShape[T] = SourceShape.of(out)

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, ActorRef) =
    throw new IllegalStateException("Not supported")

  private[akka] override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
      eagerMaterializer: Materializer): (GraphStageLogic, ActorRef) = {
    val stage: GraphStageLogic with OutHandler with StageLogging with ActorRefStage = new GraphStageLogic(shape)
      with OutHandler
      with StageLogging
      with ActorRefStage {
      override protected def logSource: Class[_] = classOf[ActorRefSource[_]]

      private val buffer: OptionVal[Buffer[T]] =
        if (maxBuffer != 0)
          OptionVal(Buffer(maxBuffer, inheritedAttributes))
        else {
          OptionVal.None // for backwards compatibility with old actor publisher based implementation
        }
      private var isCompleting: Boolean = false

      override protected def stageActorName: String = inheritedAttributes.nameForActorRef(super.stageActorName)

      private val name = inheritedAttributes.nameOrDefault(getClass.toString)
      override val ref: ActorRef = getEagerStageActor(eagerMaterializer) {
        case (_, m) if failureMatcher.isDefinedAt(m) =>
          failStage(failureMatcher(m))
        case (_, m) if completionMatcher.isDefinedAt(m) =>
          completionMatcher(m) match {
            case CompletionStrategy.Draining =>
              isCompleting = true
              tryPush()
            case CompletionStrategy.Immediately =>
              completeStage()
          }
        case (_, m: T @unchecked) =>
          buffer match {
            case OptionVal.Some(buf) =>
              if (isCompleting) {
                log.warning(
                  "Dropping element because Status.Success received already, only draining already buffered elements: [{}] (pending: [{}] in stream [{}])",
                  m,
                  buf.used,
                  name)
              } else if (!buf.isFull) {
                buf.enqueue(m)
                tryPush()
              } else
                overflowStrategy match {
                  case s: DropHead =>
                    log.log(
                      s.logLevel,
                      "Dropping the head element because buffer is full and overflowStrategy is: [DropHead] in stream [{}]",
                      name)
                    buf.dropHead()
                    buf.enqueue(m)
                    tryPush()
                  case s: DropTail =>
                    log.log(
                      s.logLevel,
                      "Dropping the tail element because buffer is full and overflowStrategy is: [DropTail] in stream [{}]",
                      name)
                    buf.dropTail()
                    buf.enqueue(m)
                    tryPush()
                  case s: DropBuffer =>
                    log.log(
                      s.logLevel,
                      "Dropping all the buffered elements because buffer is full and overflowStrategy is: [DropBuffer] in stream [{}]",
                      name)
                    buf.clear()
                    buf.enqueue(m)
                    tryPush()
                  case s: DropNew =>
                    log.log(
                      s.logLevel,
                      "Dropping the new element because buffer is full and overflowStrategy is: [DropNew] in stream [{}]",
                      name)
                  case s: Fail =>
                    log.log(
                      s.logLevel,
                      "Failing because buffer is full and overflowStrategy is: [Fail] in stream [{}]",
                      name)
                    val bufferOverflowException =
                      BufferOverflowException(s"Buffer overflow (max capacity was: $maxBuffer)!")
                    failStage(bufferOverflowException)
                  case _: Backpressure =>
                    // there is a precondition check in Source.actorRefSource factory method to not allow backpressure as strategy
                    failStage(new IllegalStateException("Backpressure is not supported"))
                }
            case _ =>
              if (isCompleting) {
                log.warning("Dropping element because Status.Success received already: [{}] in stream [{}]", m, name)
              } else if (isAvailable(out)) {
                push(out, m)
              } else {
                log.debug(
                  "Dropping element because there is no downstream demand and no buffer: [{}] in stream [{}]",
                  m,
                  name)
              }
          }

        case _ => throw new IllegalArgumentException() // won't happen, compiler exhaustiveness check pleaser
      }.ref

      private def tryPush(): Unit = {
        if (isAvailable(out) && buffer.isDefined && buffer.get.nonEmpty) {
          val msg = buffer.get.dequeue()
          push(out, msg)
        }

        if (isCompleting && (buffer.isEmpty || buffer.get.isEmpty)) {
          completeStage()
        }
      }

      override def onPull(): Unit = tryPush()

      setHandler(out, this)
    }

    (stage, stage.ref)
  }
}
