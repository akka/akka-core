/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka
package pattern

/**
 * == Extended Versions Of Akka Patterns ==
 *
 * This subpackage contains extended versions of Akka patterns.
 *
 * Currently supported are:
 *
 * <ul>
 * <li><b>ask:</b> create a temporary one-off actor for receiving a reply to a
 * message and complete a [[scala.concurrent.Future]] with it; returns said
 * Future.</li>
 * a message.</li>
 * </ul>
 *
 * In Scala the recommended usage is to import the pattern from the package
 * object:
 * {{{
 * import akka.pattern.extended.ask
 *
 * ask(actor, askSender => Request(askSender)) // use it directly
 * actor ask (Request(_))   // use it by implicit conversion
 * }}}
 *
 * For Java the patterns are available as static methods of the [[akka.pattern.Patterns]]
 * class:
 * {{{
 * import static akka.pattern.Patterns.ask;
 *
 * ask(actor, new akka.japi.Function<ActorRef, Object> {
 *   Object apply(ActorRef askSender) {
 *     return new Request(askSender);
 *   }
 * });
 * }}}
 */
package object extended extends ExplicitAskSupport
