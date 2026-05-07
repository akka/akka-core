/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import static akka.actor.SupervisorStrategy.Directive;

/**
 * Used for building a partial function for {@link akka.actor.Actor#supervisorStrategy()
 * Actor.supervisorStrategy()}. * Inside an actor you can use it like this with Java 8 to define
 * your supervisorStrategy.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Override
 * private static SupervisorStrategy strategy =
 *   new OneForOneStrategy(10, Duration.ofMinutes(1), DeciderBuilder.
 *     match(ArithmeticException.class, e -&gt; resume()).
 *     match(NullPointerException.class, e -&gt; restart()).
 *     match(IllegalArgumentException.class, e -&gt; stop()).
 *     matchAny(o -&gt; escalate()).build());
 *
 * &#64;Override
 * public SupervisorStrategy supervisorStrategy() {
 *   return strategy;
 * }
 * </pre>
 */
public class DeciderBuilder {
  private DeciderBuilder() {}

  /**
   * Return a new {@link PFBuilder} with a case statement added.
   *
   * @param type a type to match the argument against
   * @param apply an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   */
  public static <P extends Throwable> PFBuilder<Throwable, Directive> match(
      final Class<P> type, FI.Apply<P, Directive> apply) {
    return Match.match(type, apply);
  }

  /**
   * Return a new {@link PFBuilder} with a case statement added.
   *
   * @param type a type to match the argument against
   * @param predicate a predicate that will be evaluated on the argument if the type matches
   * @param apply an action to apply to the argument if the type matches and the predicate returns
   *     true
   * @return a builder with the case statement added
   */
  public static <P extends Throwable> PFBuilder<Throwable, Directive> match(
      final Class<P> type, FI.TypedPredicate<P> predicate, FI.Apply<P, Directive> apply) {
    return Match.match(type, predicate, apply);
  }

  /**
   * Return a new {@link PFBuilder} with a case statement added.
   *
   * @param apply an action to apply to the argument
   * @return a builder with the case statement added
   */
  public static PFBuilder<Throwable, Directive> matchAny(FI.Apply<Throwable, Directive> apply) {
    return Match.matchAny(apply);
  }
}
