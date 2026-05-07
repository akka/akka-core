/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf;

import akka.actor.FSM;
import java.util.List;
import scala.PartialFunction;

/**
 * Builder used to create a partial function for {@link akka.actor.FSM#whenUnhandled}.
 *
 * @param <S> the state type
 * @param <D> the data type
 */
@SuppressWarnings("rawtypes")
public class FSMStateFunctionBuilder<S, D> {

  private PFBuilder<FSM.Event<D>, FSM.State<S, D>> builder =
      new PFBuilder<FSM.Event<D>, FSM.State<S, D>>();

  /**
   * An erased processing of the event matcher. The compile time checks are enforced by the public
   * typed versions.
   *
   * <p>It works like this.
   *
   * <p>If eventOrType or dataOrType is a Class, then we do a isInstance check, otherwise we do an
   * equals check. The null value compares true for anything. If the predicate is null, it is
   * skipped otherwise the predicate has to match as well.
   *
   * @param eventOrType an event or a type to match against
   * @param dataOrType a data instance or a type to match against
   * @param predicate a predicate to match against
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  private FSMStateFunctionBuilder<S, D> erasedEvent(
      final Object eventOrType,
      final Object dataOrType,
      final FI.TypedPredicate2 predicate,
      final FI.Apply2 apply) {
    builder.match(
        FSM.Event.class,
        new FI.TypedPredicate<FSM.Event>() {
          @Override
          public boolean defined(FSM.Event e) {
            boolean res = true;
            if (eventOrType != null) {
              if (eventOrType instanceof Class) {
                Class eventType = (Class) eventOrType;
                res = eventType.isInstance(e.event());
              } else {
                res = eventOrType.equals(e.event());
              }
            }
            if (res && dataOrType != null) {
              if (dataOrType instanceof Class) {
                Class dataType = (Class) dataOrType;
                res = dataType.isInstance(e.stateData());
              } else {
                res = dataOrType.equals(e.stateData());
              }
            }
            if (res && predicate != null) {
              @SuppressWarnings("unchecked")
              boolean ures = predicate.defined(e.event(), e.stateData());
              res = ures;
            }
            return res;
          }
        },
        new FI.Apply<FSM.Event, FSM.State<S, D>>() {
          public FSM.State<S, D> apply(FSM.Event e) throws Exception {
            @SuppressWarnings("unchecked")
            FSM.State<S, D> res = (FSM.State<S, D>) apply.apply(e.event(), e.stateData());
            return res;
          }
        });

    return this;
  }

  /**
   * Add a case statement that matches on an event and data type and a predicate.
   *
   * @param eventType the event type to match on
   * @param dataType the data type to match on
   * @param predicate a predicate to evaluate on the matched types
   * @param apply an action to apply to the event and state data if there is a match
   * @param <P> the event type to match on
   * @param <Q> the data type to match on
   * @return the builder with the case statement added
   */
  public final <P, Q> FSMStateFunctionBuilder<S, D> event(
      final Class<P> eventType,
      final Class<Q> dataType,
      final FI.TypedPredicate2<P, Q> predicate,
      final FI.Apply2<P, Q, FSM.State<S, D>> apply) {
    erasedEvent(eventType, dataType, predicate, apply);
    return this;
  }

  /**
   * Add a case statement that matches on an event and data type.
   *
   * @param eventType the event type to match on
   * @param dataType the data type to match on
   * @param apply an action to apply to the event and state data if there is a match
   * @param <P> the event type to match on
   * @param <Q> the data type to match on
   * @return the builder with the case statement added
   */
  public <P, Q> FSMStateFunctionBuilder<S, D> event(
      final Class<P> eventType,
      final Class<Q> dataType,
      final FI.Apply2<P, Q, FSM.State<S, D>> apply) {
    return erasedEvent(eventType, dataType, null, apply);
  }

  /**
   * Add a case statement that matches if the event type and predicate matches.
   *
   * @param eventType the event type to match on
   * @param predicate a predicate that will be evaluated on the data and the event
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  public <P> FSMStateFunctionBuilder<S, D> event(
      final Class<P> eventType,
      final FI.TypedPredicate2<P, D> predicate,
      final FI.Apply2<P, D, FSM.State<S, D>> apply) {
    return erasedEvent(eventType, null, predicate, apply);
  }

  /**
   * Add a case statement that matches if the event type and predicate matches.
   *
   * @param eventType the event type to match on
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  public <P> FSMStateFunctionBuilder<S, D> event(
      final Class<P> eventType, final FI.Apply2<P, D, FSM.State<S, D>> apply) {
    return erasedEvent(eventType, null, null, apply);
  }

  /**
   * Add a case statement that matches if the predicate matches.
   *
   * @param predicate a predicate that will be evaluated on the data and the event
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  public FSMStateFunctionBuilder<S, D> event(
      final FI.TypedPredicate2<Object, D> predicate,
      final FI.Apply2<Object, D, FSM.State<S, D>> apply) {
    return erasedEvent(null, null, predicate, apply);
  }

  /**
   * Add a case statement that matches on the data type and if any of the event types in the list
   * match or any of the event instances in the list compares equal.
   *
   * @param eventMatches a list of types or instances to match against
   * @param dataType the data type to match on
   * @param apply an action to apply to the event and state data if there is a match
   * @param <Q> the data type to match on
   * @return the builder with the case statement added
   */
  public <Q> FSMStateFunctionBuilder<S, D> event(
      final List<Object> eventMatches,
      final Class<Q> dataType,
      final FI.Apply2<Object, Q, FSM.State<S, D>> apply) {
    builder.match(
        FSM.Event.class,
        new FI.TypedPredicate<FSM.Event>() {
          @Override
          public boolean defined(FSM.Event e) {
            if (dataType != null && !dataType.isInstance(e.stateData())) return false;

            boolean emMatch = false;
            Object event = e.event();
            for (Object em : eventMatches) {
              if (em instanceof Class) {
                Class emc = (Class) em;
                emMatch = emc.isInstance(event);
              } else {
                emMatch = event.equals(em);
              }
              if (emMatch) break;
            }
            return emMatch;
          }
        },
        new FI.Apply<FSM.Event, FSM.State<S, D>>() {
          public FSM.State<S, D> apply(FSM.Event e) throws Exception {
            @SuppressWarnings("unchecked")
            Q q = (Q) e.stateData();
            return apply.apply(e.event(), q);
          }
        });

    return this;
  }

  /**
   * Add a case statement that matches if any of the event types in the list match or any of the
   * event instances in the list compares equal.
   *
   * @param eventMatches a list of types or instances to match against
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  public FSMStateFunctionBuilder<S, D> event(
      final List<Object> eventMatches, final FI.Apply2<Object, D, FSM.State<S, D>> apply) {
    return event(eventMatches, null, apply);
  }

  /**
   * Add a case statement that matches on the data type and if the event compares equal.
   *
   * @param event an event to compare equal against
   * @param dataType the data type to match on
   * @param apply an action to apply to the event and state data if there is a match
   * @param <Q> the data type to match on
   * @return the builder with the case statement added
   */
  public <P, Q> FSMStateFunctionBuilder<S, D> eventEquals(
      final P event, final Class<Q> dataType, final FI.Apply2<P, Q, FSM.State<S, D>> apply) {
    return erasedEvent(event, dataType, null, apply);
  }

  /**
   * Add a case statement that matches if event compares equal.
   *
   * @param event an event to compare equal against
   * @param apply an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  public <P> FSMStateFunctionBuilder<S, D> eventEquals(
      final P event, final FI.Apply2<P, D, FSM.State<S, D>> apply) {
    return erasedEvent(event, null, null, apply);
  }

  /**
   * Add a case statement that matches on any type of event.
   *
   * @param apply an action to apply to the event and state data
   * @return the builder with the case statement added
   */
  public FSMStateFunctionBuilder<S, D> anyEvent(final FI.Apply2<Object, D, FSM.State<S, D>> apply) {
    return erasedEvent(null, null, null, apply);
  }

  /**
   * Build a {@link scala.PartialFunction} from this builder. After this call the builder will be
   * reset.
   *
   * @return a PartialFunction for this builder.
   */
  public PartialFunction<FSM.Event<D>, FSM.State<S, D>> build() {
    return builder.build();
  }
}
