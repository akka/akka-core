/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import scala.MatchError;
import scala.PartialFunction;

/**
 * Version of {@link scala.PartialFunction} that can be built during runtime from Java.
 *
 * @param <I> the input type, that this PartialFunction will be applied to
 * @param <R> the return type, that the results of the application will have
 */
public class Match<I, R> extends AbstractMatch<I, R> {

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added.
   *
   * @param type a type to match the argument against
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   * @see PFBuilder#match(Class, FI.Apply)
   */
  public static <F, T, P> PFBuilder<F, T> match(final Class<P> type, final FI.Apply<P, T> apply) {
    return new PFBuilder<F, T>().match(type, apply);
  }

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added without
   * compile time type check of the parameters. Should normally not be used, but when matching on
   * class with generic type argument it can be useful, e.g. <code>List.class</code> and <code>
   * (List&lt;String&gt; list) -> {}</code>.
   *
   * @see PFBuilder#matchUnchecked(Class, FI.Apply)
   */
  public static <F, T> PFBuilder<F, T> matchUnchecked(
      final Class<?> type, final FI.Apply<?, T> apply) {
    return new PFBuilder<F, T>().matchUnchecked(type, apply);
  }

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added.
   *
   * @param type a type to match the argument against
   * @param predicate a predicate that will be evaluated on the argument if the type matches
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   * @see PFBuilder#match(Class, FI.TypedPredicate, FI.Apply)
   */
  public static <F, T, P> PFBuilder<F, T> match(
      final Class<P> type, final FI.TypedPredicate<P> predicate, final FI.Apply<P, T> apply) {
    return new PFBuilder<F, T>().match(type, predicate, apply);
  }

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added without
   * compile time type check of the parameters. Should normally not be used, but when matching on
   * class with generic type argument it can be useful, e.g. <code>List.class</code> and <code>
   * (List&lt;String&gt; list) -> {}</code>.
   *
   * @see PFBuilder#matchUnchecked(Class, FI.TypedPredicate, FI.Apply)
   */
  public static <F, T> PFBuilder<F, T> matchUnchecked(
      final Class<?> type, final FI.TypedPredicate<?> predicate, final FI.Apply<?, T> apply) {
    return new PFBuilder<F, T>().matchUnchecked(type, predicate, apply);
  }

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added.
   *
   * @param object the object to compare equals with
   * @param apply an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   * @see PFBuilder#matchEquals(Object, FI.Apply)
   */
  public static <F, T, P> PFBuilder<F, T> matchEquals(final P object, final FI.Apply<P, T> apply) {
    return new PFBuilder<F, T>().matchEquals(object, apply);
  }

  /**
   * Convenience function to create a {@link PFBuilder} with the first case statement added.
   *
   * @param apply an action to apply to the argument
   * @return a builder with the case statement added
   * @see PFBuilder#matchAny(FI.Apply)
   */
  public static <F, T> PFBuilder<F, T> matchAny(final FI.Apply<F, T> apply) {
    return new PFBuilder<F, T>().matchAny(apply);
  }

  /**
   * Create a {@link Match} from the builder.
   *
   * @param builder a builder representing the partial function
   * @return a {@link Match} that can be reused
   */
  public static final <F, T> Match<F, T> create(PFBuilder<F, T> builder) {
    return new Match<F, T>(builder.build());
  }

  Match(PartialFunction<I, R> statements) {
    super(statements);
  }

  /**
   * Convenience function to make the Java code more readable.
   *
   * <p>
   *
   * <pre><code>
   *   Match&lt;X, Y&gt; matcher = Match.create(...);
   *
   *   Y someY = matcher.match(obj);
   * </code></pre>
   *
   * @param i the argument to apply the match to
   * @return the result of the application
   * @throws MatchError if there is no match
   */
  public R match(I i) throws MatchError {
    return statements.apply(i);
  }
}
