# Classic FSM

@@include[includes.md](includes.md) { #actor-api }
For the documentation of the new API of this feature and for new projects see @ref:[fsm](typed/fsm.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Finite State Machine actors, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Overview

The FSM (Finite State Machine) is available as @scala[a mixin for the] @java[an abstract base class that implements an] Akka Actor and
is best described in the [Erlang design principles](https://www.erlang.org/documentation/doc-4.8.2/doc/design_principles/fsm.html)

A FSM can be described as a set of relations of the form:

> **State(S) x Event(E) -> Actions (A), State(S')**

These relations are interpreted as meaning:

> *If we are in state S and the event E occurs, we should perform the actions A
and make a transition to the state S'.*

## A Simple Example

To demonstrate most of the features of the @scala[`FSM` trait]@java[`AbstractFSM` class], consider an
actor which shall receive and queue messages while they arrive in a burst and
send them on after the burst ended or a flush request is received.

First, consider all of the below to use these import statements:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #simple-imports }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #simple-imports }

The contract of our “Buncher” actor is that it accepts or produces the following messages:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #simple-events }

Java
:  @@snip [Events.java](/akka-docs/src/test/java/jdocs/actor/fsm/Events.java) { #simple-events }

`SetTarget` is needed for starting it up, setting the destination for the
`Batches` to be passed on; `Queue` will add to the internal queue while
`Flush` will mark the end of a burst.

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #simple-state }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #simple-state }

The actor can be in two states: no message queued (aka `Idle`) or some
message queued (aka `Active`). It will stay in the `Active` state as long as
messages keep arriving and no flush is requested. The internal state data of
the actor is made up of the target actor reference to send the batches to and
the actual queue of messages.

Now let’s take a look at the skeleton for our FSM actor:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #simple-fsm }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #simple-fsm }

The basic strategy is to declare the actor, @scala[mixing in the `FSM` trait]@java[by inheriting the `AbstractFSM` class]
and specifying the possible states and data values as type parameters. Within
the body of the actor a DSL is used for declaring the state machine:

 * `startWith` defines the initial state and initial data
 * then there is one `when(<state>) { ... }` declaration per state to be
handled (could potentially be multiple ones, the passed
`PartialFunction` will be concatenated using `orElse`)
 * finally starting it up using `initialize`, which performs the
transition into the initial state and sets up timers (if required).

In this case, we start out in the `Idle` state with `Uninitialized` data, where
only the `SetTarget()` message is handled; `stay` prepares to end this
event’s processing for not leaving the current state, while the `using`
modifier makes the FSM replace the internal state (which is `Uninitialized`
at this point) with a fresh `Todo()` object containing the target actor
reference. The `Active` state has a state timeout declared, which means that
if no message is received for 1 second, a `FSM.StateTimeout` message will be
generated. This has the same effect as receiving the `Flush` command in this
case, namely to transition back into the `Idle` state and resetting the
internal queue to the empty vector. But how do messages get queued? Since this
shall work identically in both states, we make use of the fact that any event
which is not handled by the `when()` block is passed to the
`whenUnhandled()` block:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #unhandled-elided }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #unhandled-elided }

The first case handled here is adding `Queue()` requests to the internal
queue and going to the `Active` state (this does the obvious thing of staying
in the `Active` state if already there), but only if the FSM data are not
`Uninitialized` when the `Queue()` event is received. Otherwise—and in all
other non-handled cases—the second case just logs a warning and does not change
the internal state.

The only missing piece is where the `Batches` are actually sent to the
target, for which we use the `onTransition` mechanism: you can declare
multiple such blocks and all of them will be tried for matching behavior in
case a state transition occurs (i.e. only when the state actually changes).

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #transition-elided }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #transition-elided }

The transition callback is a @scala[partial function]@java[builder constructed by `matchState`, followed by zero or multiple `state`], which takes as input a pair of
states—the current and the next state. @scala[The FSM trait includes a convenience
extractor for these in form of an arrow operator, which conveniently reminds
you of the direction of the state change which is being matched.] During the 
state change, the old state data is available via @scala[`stateData`]@java[`stateData()`] as shown, and
the new state data would be available as @scala[`nextStateData`]@java[`nextStateData()`].

@@@ note

Same-state transitions can be implemented (when currently in state `S`) using
`goto(S)` or `stay()`. The difference between those being that `goto(S)` will
emit an event `S->S` event that can be handled by `onTransition`,
whereas `stay()` will *not*.

@@@

To verify that this buncher actually works, it is quite easy to write a test
using the @scala[@ref:[Testing Actor Systems which is conveniently bundled with ScalaTest traits into `AkkaSpec`](testing.md)]@java[@ref:[TestKit](testing.md), here using JUnit as an example]:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #test-code }

Java
:  @@snip [BuncherTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/BuncherTest.java) { #test-code }

## Reference

### The @scala[FSM Trait and Object]@java[AbstractFSM Class]

@scala[
The `FSM` trait inherits directly from `Actor`, when you
extend `FSM` you must be aware that an actor is actually created:
]
@java[
The `AbstractFSM` abstract class is the base class used to implement an FSM. It implements
Actor since an Actor is created to drive the FSM.
]

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #simple-fsm }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #simple-fsm }

@@@ note

The @scala[`FSM` trait]@java[`AbstractFSM` class] defines a `receive` method which handles internal messages
and passes everything else through to the FSM logic (according to the
current state). When overriding the `receive` method, keep in mind that
e.g. state timeout handling depends on actually passing the messages through
the FSM logic.

@@@

The @scala[`FSM` trait]@java[`AbstractFSM` class] takes two type parameters:

 1. the supertype of all state names, usually @scala[a sealed trait with case objects extending it]@java[an enum]
 2. the type of the state data which are tracked by the @scala[`FSM`]@java[`AbstractFSM`]  module
itself.

@@@ note

The state data together with the state name describe the internal state of
the state machine; if you stick to this scheme and do not add mutable fields
to the FSM class you have the advantage of making all changes of the
internal state explicit in a few well-known places.

@@@

### Defining States

A state is defined by one or more invocations of the method

```
when(<name>[, stateTimeout = <timeout>])(stateFunction)
```

The given name must be an object which is type-compatible with the first type
parameter given to the @scala[`FSM` trait]@java[`AbstractFSM` class]. This object is used as a hash key,
so you must ensure that it properly implements `equals` and
`hashCode`; in particular it must not be mutable. The easiest fit for
these requirements are case objects.

If the `stateTimeout` parameter is given, then all transitions into this
state, including staying, receive this timeout by default. Initiating the
transition with an explicit timeout may be used to override this default, see
[Initiating Transitions](#initiating-transitions) for more information. The state timeout of any state
may be changed during action processing with
`setStateTimeout(state, duration)`. This enables runtime configuration
e.g. via external message.

The `stateFunction` argument is a `PartialFunction[Event, State]`,
which is conveniently given using the @scala[partial function literal]@java[state function builder] syntax as
demonstrated below:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #when-syntax }

Java
:  @@snip [Buncher.java](/akka-docs/src/test/java/jdocs/actor/fsm/Buncher.java) { #when-syntax }

@@@ div { .group-scala }

The `Event(msg: Any, data: D)` case class is parameterized with the data
type held by the FSM for convenient pattern matching.

@@@

@@@ warning

It is required that you define handlers for each of the possible FSM states,
otherwise there will be failures when trying to switch to undeclared states.

@@@

It is recommended practice to declare the states as @scala[objects extending a sealed trait]@java[an enum]
and then verify that there is a `when` clause for each of the
states. If you want to leave the handling of a state “unhandled” (more below),
it still needs to be declared like this:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #NullFunction }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #NullFunction }

### Defining the Initial State

Each FSM needs a starting point, which is declared using

```
startWith(state, data[, timeout])
```

The optionally given timeout argument overrides any specification given for the
desired initial state. If you want to cancel a default timeout, use
@scala[`None`]@java[`Duration.Inf`].

### Unhandled Events

If a state doesn't handle a received event a warning is logged. If you want to
do something else in this case you can specify that with
`whenUnhandled(stateFunction)`:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #unhandled-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #unhandled-syntax }

Within this handler the state of the FSM may be queried using the
`stateName` method.

**IMPORTANT**: This handler is not stacked, meaning that each invocation of
`whenUnhandled` replaces the previously installed handler.

### Initiating Transitions

The result of any `stateFunction` must be a definition of the next state
unless terminating the FSM, which is described in @ref:[Termination from Inside](#termination-from-inside).
The state definition can either be the current state, as described by the
`stay` directive, or it is a different state as given by
`goto(state)`. The resulting object allows further qualification by way
of the modifiers described in the following:

 * 
   `forMax(duration)`
   This modifier sets a state timeout on the next state. This means that a timer
is started which upon expiry sends a `StateTimeout` message to the FSM.
This timer is canceled upon reception of any other message in the meantime;
you can rely on the fact that the `StateTimeout` message will not be
processed after an intervening message.
   This modifier can also be used to override any default timeout which is
specified for the target state. If you want to cancel the default timeout,
use `Duration.Inf`.
 * 
   `using(data)`
   This modifier replaces the old state data with the new data given. If you
follow the advice @ref:[above](#defining-states), this is the only place where
internal state data are ever modified.
 * 
   `replying(msg)`
   This modifier sends a reply to the currently processed message and otherwise
does not modify the state transition.

All modifiers can be chained to achieve a nice and concise description:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #modifier-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #modifier-syntax }

The parentheses are not actually needed in all cases, but they visually
distinguish between modifiers and their arguments and therefore make the code
even more pleasant to read.

@@@ note

Please note that the `return` statement may not be used in `when`
blocks or similar; this is a Scala restriction. Either refactor your code
using `if () ... else ...` or move it into a method definition.

@@@

### Monitoring Transitions

Transitions occur "between states" conceptually, which means after any actions
you have put into the event handling block; this is obvious since the next
state is only defined by the value returned by the event handling logic. You do
not need to worry about the exact order with respect to setting the internal
state variable, as everything within the FSM actor is running single-threaded
anyway.

#### Internal Monitoring

Up to this point, the FSM DSL has been centered on states and events. The dual
view is to describe it as a series of transitions. This is enabled by the
method

```
onTransition(handler)
```

which associates actions with a transition instead of with a state and event.
The handler is a partial function which takes a pair of states as input; no
resulting state is needed as it is not possible to modify the transition in
progress.

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #transition-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #transition-syntax }

@@@ div { .group-scala }

The convenience extractor `->` enables decomposition of the pair of states
with a clear visual reminder of the transition's direction. As usual in pattern
matches, an underscore may be used for irrelevant parts; alternatively you
could bind the unconstrained state to a variable, e.g. for logging as shown in
the last case.

@@@

It is also possible to pass a function object accepting two states to
`onTransition`, in case your transition handling logic is implemented as
a method:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #alt-transition-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #alt-transition-syntax }

The handlers registered with this method are stacked, so you can intersperse
`onTransition` blocks with `when` blocks as suits your design. It
should be noted, however, that *all handlers will be invoked for each
transition*, not only the first matching one. This is designed specifically so
you can put all transition handling for a certain aspect into one place without
having to worry about earlier declarations shadowing later ones; the actions
are still executed in declaration order, though.

@@@ note

This kind of internal monitoring may be used to structure your FSM according
to transitions, so that for example the cancellation of a timer upon leaving
a certain state cannot be forgot when adding new target states.

@@@

#### External Monitoring

External actors may be registered to be notified of state transitions by
sending a message `SubscribeTransitionCallBack(actorRef)`. The named
actor will be sent a `CurrentState(self, stateName)` message immediately
and will receive `Transition(actorRef, oldState, newState)` messages
whenever a state change is triggered.

@@@ div { .group-scala }

Please note that a state change includes the action of performing an `goto(S)`, while
already being state `S`. In that case the monitoring actor will be notified with an
`Transition(ref,S,S)` message. This may be useful if your `FSM` should
react on all (also same-state) transitions. In case you'd rather not emit events for same-state
transitions use `stay()` instead of `goto(S)`.

@@@

External monitors may be unregistered by sending
`UnsubscribeTransitionCallBack(actorRef)` to the `FSM` actor.

Stopping a listener without unregistering will not remove the listener from the
subscription list; use `UnsubscribeTransitionCallback` before stopping
the listener.

@@@ div { .group-scala }

### Transforming State

The partial functions supplied as argument to the `when()` blocks can be
transformed using Scala’s full supplement of functional programming tools. In
order to retain type inference, there is a helper function which may be used in
case some common handling logic shall be applied to different clauses:

@@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #transform-syntax }

It goes without saying that the arguments to this method may also be stored, to
be used several times, e.g. when applying the same transformation to several
`when()` blocks:

@@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #alt-transform-syntax }

@@@

### Timers

Besides state timeouts, FSM manages timers identified by `String` names.
You may set a timer using

```
startSingleTimer(name, msg, interval)
startTimerWithFixedDelay(name, msg, interval)
```

where `msg` is the message object which will be sent after the duration
`interval` has elapsed.

Any existing timer with the same name will automatically be canceled before
adding the new timer.

The @ref:[Scheduler](scheduler.md#schedule-periodically) documentation describes the difference between
`fixed-delay` and `fixed-rate` scheduling. If you are uncertain of which one to use you should pick
`startTimerWithFixedDelay`.

Timers may be canceled using

```
cancelTimer(name)
```

which is guaranteed to work immediately, meaning that the scheduled message
will not be processed after this call even if the timer already fired and
queued it. The status of any timer may be inquired with

```
isTimerActive(name)
```

These named timers complement state timeouts because they are not affected by
intervening reception of other messages.

### Termination from Inside

The FSM is stopped by specifying the result state as

```
stop([reason[, data]])
```

The reason must be one of `Normal` (which is the default), `Shutdown`
or `Failure(reason)`, and the second argument may be given to change the
state data which is available during termination handling.

@@@ note

It should be noted that `stop` does not abort the actions and stop the
FSM immediately. The stop action must be returned from the event handler in
the same way as a state transition (but note that the `return` statement
may not be used within a `when` block).

@@@

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #stop-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #stop-syntax }

You can use `onTermination(handler)` to specify custom code that is
executed when the FSM is stopped. The handler is a partial function which takes
a `StopEvent(reason, stateName, stateData)` as argument:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #termination-syntax }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #termination-syntax }

As for the `whenUnhandled` case, this handler is not stacked, so each
invocation of `onTermination` replaces the previously installed handler.

### Termination from Outside

When an `ActorRef` associated to a FSM is stopped using the
`stop()` method, its `postStop` hook will be executed. The default
implementation by the @scala[`FSM` trait]@java[`AbstractFSM` class] is to execute the
`onTermination` handler if that is prepared to handle a
`StopEvent(Shutdown, ...)`.

@@@ warning

In case you override `postStop` and want to have your
`onTermination` handler called, do not forget to call
`super.postStop`.

@@@

## Testing and Debugging Finite State Machines

During development and for trouble shooting FSMs need care just as any other
actor. There are specialized tools available as described in @ref:[TestFSMRef](testing.md#testfsmref)
and in the following.

### Event Tracing

The setting `akka.actor.debug.fsm` in @ref:[configuration](general/configuration.md) enables logging of an
event trace by `LoggingFSM` instances:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #logging-fsm }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #logging-fsm }

This FSM will log at DEBUG level:

 * all processed events, including `StateTimeout` and scheduled timer
messages
 * every setting and cancellation of named timers
 * all state transitions

Life cycle changes and special messages can be logged as described for
@ref:[Actors](testing.md#actor-logging).

### Rolling Event Log

The @scala[`LoggingFSM` trait]@java[`AbstractLoggingFSM` class] adds one more feature to the FSM: a rolling event
log which may be used during debugging (for tracing how the FSM entered a
certain failure state) or for other creative uses:

Scala
:  @@snip [FSMDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FSMDocSpec.scala) { #logging-fsm }

Java
:  @@snip [FSMDocTest.java](/akka-docs/src/test/java/jdocs/actor/fsm/FSMDocTest.java) { #logging-fsm }

The `logDepth` defaults to zero, which turns off the event log.

@@@ warning

The log buffer is allocated during actor creation, which is why the
configuration is done using a virtual method call. If you want to override
with a `val`, make sure that its initialization happens before the
initializer of `LoggingFSM` runs, and do not change the value returned
by `logDepth` after the buffer has been allocated.

@@@

The contents of the event log are available using method `getLog`, which
returns an `IndexedSeq[LogEntry]` where the oldest entry is at index
zero.
