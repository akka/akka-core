/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import scala.PartialFunction;

/**
 * A builder for {@link scala.PartialFunction}.
 *
 * @param <F> the input type, that this PartialFunction will be applied to
 * @param <T> the return type, that the results of the application will have
 */
abstract class AbstractPFBuilder<F, T> {

  protected PartialFunction<F, T> statements = null;

  protected void addStatement(PartialFunction<F, T> statement) {
    if (statements == null) statements = statement;
    else statements = statements.orElse(statement);
  }

  /**
   * Build a {@link scala.PartialFunction} from this builder. After this call the builder will be
   * reset.
   *
   * @return a PartialFunction for this builder.
   */
  public PartialFunction<F, T> build() {
    PartialFunction<F, T> empty = CaseStatement.empty();
    PartialFunction<F, T> statements = this.statements;

    this.statements = null;
    if (statements == null) return empty;
    else return statements.orElse(empty);
  }
}
