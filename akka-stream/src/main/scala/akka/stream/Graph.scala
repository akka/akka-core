/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance

import akka.annotation.InternalApi
import akka.stream.impl.TraversalBuilder
import akka.stream.scaladsl.GenericGraph

/**
 * Not intended to be directly extended by user classes
 *
 * @see [[akka.stream.stage.GraphStage]]
 */
trait Graph[+S <: Shape, +M] {

  /**
   * Type-level accessor for the shape parameter of this graph.
   */
  type Shape = S @uncheckedVariance

  /**
   * The shape of a graph is all that is externally visible: its inlets and outlets.
   */
  def shape: S

  /**
   * INTERNAL API.
   *
   * Every materializable element must be backed by a stream layout module
   */
  private[stream] def traversalBuilder: TraversalBuilder

  /**
   * Replace the attributes of this [[Flow]] with the given ones. If this Flow is a composite
   * of multiple graphs, new attributes on the composite will be less specific than attributes
   * set directly on the individual graphs of the composite.
   */
  def withAttributes(attr: Attributes): Graph[S, M]

  /**
   * Specifies the name of the Graph.
   * If the name is null or empty the name is ignored, i.e. [[#none]] is returned.
   */
  def named(name: String): Graph[S, M] = addAttributes(Attributes.name(name))

  /**
   * Put an asynchronous boundary around this `Graph`
   */
  def async: Graph[S, M] = addAttributes(Attributes.asyncBoundary)

  /**
   * Put an asynchronous boundary around this `Graph`
   *
   * @param dispatcher Run the graph on this dispatcher
   */
  def async(dispatcher: String): Graph[S, M] =
    addAttributes(Attributes.asyncBoundary and ActorAttributes.dispatcher(dispatcher))

  /**
   * Put an asynchronous boundary around this `Graph`
   *
   * @param dispatcher Run the graph on this dispatcher
   * @param inputBufferSize Set the input buffer to this size for the graph
   */
  def async(dispatcher: String, inputBufferSize: Int): Graph[S, M] =
    addAttributes(
      Attributes.asyncBoundary and ActorAttributes.dispatcher(dispatcher)
      and Attributes.inputBuffer(inputBufferSize, inputBufferSize))

  /**
   * Add the given attributes to this [[Graph]]. If the specific attribute was already present
   * on this graph this means the added attribute will be more specific than the existing one.
   * If this Source is a composite of multiple graphs, new attributes on the composite will be
   * less specific than attributes set directly on the individual graphs of the composite.
   */
  def addAttributes(attr: Attributes): Graph[S, M] = withAttributes(traversalBuilder.attributes and attr)

  def getAttributes: Attributes = traversalBuilder.attributes

}

object Graph {

  /**
   * Java API
   * Transform the materialized value of this Flow, leaving all other properties as they were.
   *
   * @param g the graph being transformed
   * @param f function to map the graph's materialized value
   * @return a graph with same semantics as the given graph, except from the materialized value which is mapped using f.
   */
  def mapMaterializedValue[S <: Shape, M1, M2](g: Graph[S, M1])(f: M1 => M2): Graph[S, M2] =
    new GenericGraph(g.shape, g.traversalBuilder).mapMaterializedValue(f)

  /**
   * Scala API, see https://github.com/akka/akka/issues/28501 for discussion why this can't be an instance method on class Graph.
   * @param self the graph whose materialized value will be mapped
   */
  final implicit class GraphMapMatVal[S <: Shape, M](self: Graph[S, M]) {

    /**
     * Transform the materialized value of this Graph, leaving all other properties as they were.
     *
     * @param f function to map the graph's materialized value
     */
    def mapMaterializedValue[M2](f: M => M2): Graph[S, M2] = Graph.mapMaterializedValue(self)(f)
  }
}

/**
 * INTERNAL API
 *
 * Allows creating additional API on top of an existing Graph by extending from this class and
 * accessing the delegate
 */
@InternalApi
private[stream] abstract class GraphDelegate[+S <: Shape, +Mat](delegate: Graph[S, Mat]) extends Graph[S, Mat] {
  final override def shape: S = delegate.shape
  final override private[stream] def traversalBuilder: TraversalBuilder = delegate.traversalBuilder
}
