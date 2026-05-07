/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import scala.MatchError;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

/**
 * Version of {@link scala.PartialFunction} that can be built during runtime from Java. This is a
 * specialized version of {@link UnitMatch} to map java void methods to {@link
 * scala.runtime.BoxedUnit}.
 *
 * @param <I> the input type, that this PartialFunction will be applied to
 */
public class UnitMatch<I> extends AbstractMatch<I, BoxedUnit> {

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   *
   * @param type a type to match the argument against
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   * @see UnitPFBuilder#match(Class, FI.UnitApply)
   */
  public static <F, P> UnitPFBuilder<F> match(final Class<P> type, FI.UnitApply<P> apply) {
    return new UnitPFBuilder<F>().match(type, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   * Should normally not be used.
   *
   * @see UnitPFBuilder#matchUnchecked(Class, FI.UnitApply)
   */
  public static UnitPFBuilder<Object> matchUnchecked(
      final Class<?> type, final FI.UnitApply<?> apply) {
    return new UnitPFBuilder<Object>().matchUnchecked(type, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   *
   * @param type a type to match the argument against
   * @param predicate a predicate that will be evaluated on the argument if the type matches
   * @param apply an action to apply to the argument if the type and predicate matches
   * @return a builder with the case statement added
   * @see UnitPFBuilder#match(Class, FI.TypedPredicate, FI.UnitApply)
   */
  public static <F, P> UnitPFBuilder<F> match(
      final Class<P> type, final FI.TypedPredicate<P> predicate, final FI.UnitApply<P> apply) {
    return new UnitPFBuilder<F>().match(type, predicate, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   * Should normally not be used.
   *
   * @see UnitPFBuilder#matchUnchecked(Class, FI.TypedPredicate, FI.UnitApply)
   */
  public static <F, P> UnitPFBuilder<F> matchUnchecked(
      final Class<?> type, final FI.TypedPredicate<?> predicate, final FI.UnitApply<?> apply) {
    return new UnitPFBuilder<F>().matchUnchecked(type, predicate, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   *
   * @param object the object to compare equals with
   * @param apply an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   * @see UnitPFBuilder#matchEquals(Object, FI.UnitApply)
   */
  public static <F, P> UnitPFBuilder<F> matchEquals(final P object, final FI.UnitApply<P> apply) {
    return new UnitPFBuilder<F>().matchEquals(object, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   *
   * @param object the object to compare equals with
   * @param predicate a predicate that will be evaluated on the argument the object compares equal
   * @param apply an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   * @see UnitPFBuilder#matchEquals(Object, FI.UnitApply)
   */
  public static <F, P> UnitPFBuilder<F> matchEquals(
      final P object, final FI.TypedPredicate<P> predicate, final FI.UnitApply<P> apply) {
    return new UnitPFBuilder<F>().matchEquals(object, predicate, apply);
  }

  /**
   * Convenience function to create a {@link UnitPFBuilder} with the first case statement added.
   *
   * @param apply an action to apply to the argument
   * @return a builder with the case statement added
   * @see UnitPFBuilder#matchAny(FI.UnitApply)
   */
  public static <F> UnitPFBuilder<F> matchAny(final FI.UnitApply<Object> apply) {
    return new UnitPFBuilder<F>().matchAny(apply);
  }

  /**
   * Create a {@link UnitMatch} from the builder.
   *
   * @param builder a builder representing the partial function
   * @return a {@link UnitMatch} that can be reused
   */
  public static <F> UnitMatch<F> create(UnitPFBuilder<F> builder) {
    return new UnitMatch<F>(builder.build());
  }

  private UnitMatch(PartialFunction<I, BoxedUnit> statements) {
    super(statements);
  }

  /**
   * Convenience function to make the Java code more readable.
   *
   * <p>
   *
   * <pre><code>
   *   UnitMatcher&lt;X&gt; matcher = UnitMatcher.create(...);
   *
   *   matcher.match(obj);
   * </code></pre>
   *
   * @param i the argument to apply the match to
   * @throws scala.MatchError if there is no match
   */
  public void match(I i) throws MatchError {
    statements.apply(i);
  }
}
