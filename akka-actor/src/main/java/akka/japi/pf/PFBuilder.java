/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

/**
 * A builder for {@link scala.PartialFunction}.
 *
 * @param <I> the input type, that this PartialFunction will be applied to
 * @param <R> the return type, that the results of the application will have
 */
public final class PFBuilder<I, R> extends AbstractPFBuilder<I, R> {

  /** Create a PFBuilder. */
  public PFBuilder() {}

  /**
   * Add a new case statement to this builder.
   *
   * @param type a type to match the argument against
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   */
  public <P> PFBuilder<I, R> match(final Class<P> type, FI.Apply<P, R> apply) {
    return matchUnchecked(type, apply);
  }

  /**
   * Add a new case statement to this builder without compile time type check of the parameters.
   * Should normally not be used, but when matching on class with generic type argument it can be
   * useful, e.g. <code>List.class</code> and <code>(List&lt;String&gt; list) -> {}</code>.
   *
   * @param type a type to match the argument against
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   */
  @SuppressWarnings("unchecked")
  public PFBuilder<I, R> matchUnchecked(final Class<?> type, FI.Apply<?, R> apply) {

    FI.Predicate predicate =
        new FI.Predicate() {
          @Override
          public boolean defined(Object o) {
            return type.isInstance(o);
          }
        };

    addStatement(new CaseStatement<I, Object, R>(predicate, (FI.Apply<Object, R>) apply));
    return this;
  }

  /**
   * Add a new case statement to this builder.
   *
   * @param type a type to match the argument against
   * @param predicate a predicate that will be evaluated on the argument if the type matches
   * @param apply an action to apply to the argument if the type matches and the predicate returns
   *     true
   * @return a builder with the case statement added
   */
  public <P> PFBuilder<I, R> match(
      final Class<P> type, final FI.TypedPredicate<P> predicate, final FI.Apply<P, R> apply) {
    return matchUnchecked(type, predicate, apply);
  }

  /**
   * Add a new case statement to this builder without compile time type check of the parameters.
   * Should normally not be used, but when matching on class with generic type argument it can be
   * useful, e.g. <code>List.class</code> and <code>(List&lt;String&gt; list) -> {}</code>.
   *
   * @param type a type to match the argument against
   * @param predicate a predicate that will be evaluated on the argument if the type matches
   * @param apply an action to apply to the argument if the type matches and the predicate returns
   *     true
   * @return a builder with the case statement added
   */
  @SuppressWarnings("unchecked")
  public PFBuilder<I, R> matchUnchecked(
      final Class<?> type, final FI.TypedPredicate<?> predicate, final FI.Apply<?, R> apply) {
    FI.Predicate fiPredicate =
        new FI.Predicate() {
          @Override
          public boolean defined(Object o) {
            if (!type.isInstance(o)) return false;
            else return ((FI.TypedPredicate<Object>) predicate).defined(o);
          }
        };
    addStatement(new CaseStatement<I, Object, R>(fiPredicate, (FI.Apply<Object, R>) apply));
    return this;
  }

  /**
   * Add a new case statement to this builder.
   *
   * @param object the object to compare equals with
   * @param apply an action to apply to the argument if the object compares equal
   * @return a builder with the case statement added
   */
  public <P> PFBuilder<I, R> matchEquals(final P object, final FI.Apply<P, R> apply) {
    addStatement(
        new CaseStatement<I, P, R>(
            new FI.Predicate() {
              @Override
              public boolean defined(Object o) {
                return object.equals(o);
              }
            },
            apply));
    return this;
  }

  /**
   * Add a new case statement to this builder, that matches any argument.
   *
   * @param apply an action to apply to the argument
   * @return a builder with the case statement added
   */
  public PFBuilder<I, R> matchAny(final FI.Apply<I, R> apply) {
    addStatement(
        new CaseStatement<I, I, R>(
            new FI.Predicate() {
              @Override
              public boolean defined(Object o) {
                return true;
              }
            },
            apply));
    return this;
  }
}
