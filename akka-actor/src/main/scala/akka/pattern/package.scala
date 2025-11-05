/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

/**
 * == Commonly Used Patterns With Akka ==
 *
 * This package is used as a collection point for usage patterns which involve
 * actors, futures, etc. but are loosely enough coupled to (multiple of) them
 * to present them separately from the core implementation. Currently supported
 * are:
 *
 * <ul>
 * <li><b>ask:</b> create a temporary one-off actor for receiving a reply to a
 * message and complete a [[scala.concurrent.Future]] with it; returns said
 * Future.</li>
 * <li><b>pipeTo:</b> feed eventually computed value of a future to an [[akka.actor.ActorRef]] as
 * a message.</li>
 * <li><b>pipeToSelection:</b> feed eventually computed value of a future to an [[akka.actor.ActorSelection]] as
 * a message.</li>
 * </ul>
 *
 * In Scala the recommended usage is to import the pattern from the package
 * object:
 * {{{
 * import akka.pattern.ask
 *
 * ask(actor, message) // use it directly
 * actor ask message   // use it by implicit conversion
 * }}}
 *
 * For Java the patterns are available as static methods of the [[akka.pattern.Patterns]]
 * class:
 * {{{
 * import static akka.pattern.Patterns.ask;
 *
 * ask(actor, message);
 * }}}
 */
package object pattern
    extends PipeToSupport
    with AskSupport
    with GracefulStopSupport
    with FutureTimeoutSupport
    with RetrySupport
