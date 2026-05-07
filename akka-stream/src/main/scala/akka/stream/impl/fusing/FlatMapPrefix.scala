/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.collection.immutable
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler }
import akka.util.OptionVal

@InternalApi private[akka] final class FlatMapPrefix[In, Out, M](n: Int, f: immutable.Seq[In] => Flow[In, Out, M])
    extends GraphStageWithMaterializedValue[FlowShape[In, Out], Future[M]] {

  require(n >= 0, s"FlatMapPrefix: n ($n) must be non-negative.")

  val in = Inlet[In]("FlatMapPrefix.in")
  val out = Outlet[Out]("FlatMapPrefix.out")
  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.flatMapPrefix and SourceLocation.forLambda(f)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[M]) = {
    val propagateToNestedMaterialization =
      inheritedAttributes
        .mandatoryAttribute[Attributes.NestedMaterializationCancellationPolicy]
        .propagateToNestedMaterialization
    val matPromise = Promise[M]()
    val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
      val accumulated = collection.mutable.Buffer.empty[In]

      private var subSource = OptionVal.none[SubSourceOutlet[In]]
      private var subSink = OptionVal.none[SubSinkInlet[Out]]

      private var downstreamCause = OptionVal.none[Throwable]

      setHandlers(in, out, this)

      override def postStop(): Unit = {
        //this covers the case when the nested flow was never materialized
        if (!matPromise.isCompleted) {
          matPromise.failure(new AbruptStageTerminationException(this))
        }
        super.postStop()
      }

      override def onPush(): Unit = {
        subSource match {
          case OptionVal.Some(s) => s.push(grab(in))
          case _ =>
            accumulated.append(grab(in))
            if (accumulated.size == n) {
              materializeFlow()
            } else {
              //gi'me some more!
              pull(in)
            }
        }
      }

      override def onUpstreamFinish(): Unit = {
        subSource match {
          case OptionVal.Some(s) => s.complete()
          case _                 => materializeFlow()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        subSource match {
          case OptionVal.Some(s) => s.fail(ex)
          case _                 =>
            //flow won't be materialized, so we have to complete the future with a failure indicating this
            matPromise.failure(new NeverMaterializedException(ex))
            super.onUpstreamFailure(ex)
        }
      }

      override def onPull(): Unit = {
        subSink match {
          case OptionVal.Some(s) =>
            //delegate to subSink
            s.pull()
          case _ =>
            if (accumulated.size < n) pull(in)
            else if (accumulated.size == n) {
              //corner case for n = 0, can be handled in FlowOps
              materializeFlow()
            } else {
              throw new IllegalStateException(s"Unexpected accumulated size: ${accumulated.size} (n: $n)")
            }
        }
      }

      override def onDownstreamFinish(cause: Throwable): Unit =
        subSink match {
          case OptionVal.Some(s) => s.cancel(cause)
          case _ =>
            if (propagateToNestedMaterialization) {
              downstreamCause = OptionVal.Some(cause)
              if (accumulated.size == n) {
                //corner case for n = 0, can be handled in FlowOps
                materializeFlow()
              } else if (!hasBeenPulled(in)) { //if in was already closed, nested flow would have already been materialized
                pull(in)
              }
            } else {
              matPromise.failure(new NeverMaterializedException(cause))
              cancelStage(cause)
            }
        }

      def materializeFlow(): Unit =
        try {
          val prefix = accumulated.toVector
          accumulated.clear()
          subSource = OptionVal.Some(new SubSourceOutlet[In]("FlatMapPrefix.subSource"))
          val theSubSource = subSource.get
          theSubSource.setHandler {
            new OutHandler {
              override def onPull(): Unit = {
                if (!isClosed(in) && !hasBeenPulled(in)) {
                  pull(in)
                }
              }

              override def onDownstreamFinish(cause: Throwable): Unit = {
                if (!isClosed(in)) {
                  cancel(in, cause)
                }
              }
            }
          }
          subSink = OptionVal.Some(new SubSinkInlet[Out]("FlatMapPrefix.subSink"))
          val theSubSink = subSink.get
          theSubSink.setHandler {
            new InHandler {
              override def onPush(): Unit = {
                push(out, theSubSink.grab())
              }

              override def onUpstreamFinish(): Unit = {
                complete(out)
              }

              override def onUpstreamFailure(ex: Throwable): Unit = {
                fail(out, ex)
              }
            }
          }
          val matVal = try {
            val flow = f(prefix)
            val runnableGraph = Source.fromGraph(theSubSource.source).viaMat(flow)(Keep.right).to(theSubSink.sink)
            interpreter.subFusingMaterializer.materialize(runnableGraph, inheritedAttributes)
          } catch {
            case NonFatal(ex) =>
              matPromise.failure(new NeverMaterializedException(ex))
              subSource = OptionVal.None
              subSink = OptionVal.None
              throw ex
          }
          matPromise.success(matVal)

          //in case downstream was closed
          downstreamCause match {
            case OptionVal.Some(ex) => theSubSink.cancel(ex)
            case _                  =>
          }

          //in case we've materialized due to upstream completion
          if (isClosed(in)) {
            theSubSource.complete()
          }

          //in case we've been pulled by downstream
          if (isAvailable(out)) {
            theSubSink.pull()
          }
        } catch {
          case NonFatal(ex) => failStage(ex)
        }
    }
    (logic, matPromise.future)
  }
}
