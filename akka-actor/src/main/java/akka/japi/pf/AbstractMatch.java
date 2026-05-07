/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import scala.PartialFunction;

/**
 * Version of {@link scala.PartialFunction} that can be built during runtime from Java.
 *
 * @param <I> the input type, that this PartialFunction will be applied to
 * @param <R> the return type, that the results of the application will have
 */
class AbstractMatch<I, R> {

  protected final PartialFunction<I, R> statements;

  AbstractMatch(PartialFunction<I, R> statements) {
    PartialFunction<I, R> empty = CaseStatement.empty();
    if (statements == null) this.statements = empty;
    else this.statements = statements.orElse(empty);
  }

  /**
   * Turn this {@link Match} into a {@link scala.PartialFunction}.
   *
   * @return a partial function representation ot his {@link Match}
   */
  public PartialFunction<I, R> asPF() {
    return statements;
  }
}
