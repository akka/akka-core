# Testing Classic Actors

@@include[includes.md](includes.md) { #actor-api }
For the new API see @ref[testing](typed/testing.md).

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Akka Testkit, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-testkit_$scala.binary.version$"
  version=AkkaVersion
  scope="test"
}

@@project-info{ projectId="akka-testkit" }

## Introduction

As with any piece of software, automated tests are a very important part of the
development cycle. The actor model presents a different view on how units of
code are delimited and how they interact, which influences how to
perform tests.

Akka comes with a dedicated module `akka-testkit` for supporting tests.

<a id="async-integration-testing"></a>
## Asynchronous Testing: `TestKit`

Testkit allows you to test your actors in a controlled but realistic
environment. The definition of the environment depends very much on
the problem at hand and the level at which you intend to test, ranging from
simple checks to full system tests.

The minimal setup consists of the test procedure, which provides
the desired stimuli, the actor under test, and an actor receiving replies.
Bigger systems replace the actor under test with a network of actors, apply
stimuli at varying injection points and arrange results to be sent from
different emission points, but the basic principle stays the same in that a
single procedure drives the test.

The `TestKit` class contains a collection of tools which makes this
common task easy.

Scala
:   @@snip [PlainWordSpec.scala](/akka-docs/src/test/scala/docs/testkit/PlainWordSpec.scala) { #plain-spec }

Java
:   @@snip [TestKitSampleTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitSampleTest.java) { #fullsample }

The `TestKit` contains an actor named `testActor` which is the
entry point for messages to be examined with the various `expectMsg...`
assertions detailed below. @scala[When mixing in the trait `ImplicitSender` this
test actor is implicitly used as sender reference when dispatching messages
from the test procedure.] @java[The test actor’s reference is obtained using the
`getRef()` method as demonstrated above.] The `testActor` may also be passed to
other actors, as usual, usually subscribing it as notification listener. There
is a whole set of examination methods, e.g. receiving all consecutive messages
matching certain criteria, receiving a whole sequence of fixed messages or
classes, receiving nothing for some time, etc.

The ActorSystem passed to the constructor of TestKit is accessible via the
@scala[`system` member]@java[`getSystem()` method].

@@@ note

Remember to shut down the actor system after the test is
finished (also in case of failure) so that all actors—including the test
actor—are stopped.

@@@

### Built-In Assertions

The above-mentioned @scala[`expectMsg`]@java[`expectMsgEquals`] is not the only method for formulating
assertions concerning received messages, the full set is this:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-expect }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-expect }

In these examples, the maximum durations you will find mentioned below are left
out, in which case they use the default value from the configuration item
`akka.test.single-expect-default` which itself defaults to 3 seconds (or they
obey the innermost enclosing `Within` as detailed @ref:[below](#testkit-within)). The full signatures are:

* @scala[`expectMsg[T](d: Duration, msg: T): T`]@java[`public <T> T expectMsgEquals(Duration max, T msg)`]
   The given message object must be received within the specified time; the
object will be returned.
* @scala[`expectMsgPF[T](d: Duration)(pf: PartialFunction[Any, T]): T`]@java[`public <T> T expectMsgPF(Duration max, String hint, Function<Object, T> f)`]
   Within the given time, a message must be received and the given
@scala[partial] function must be defined for that message; the result from applying
the @scala[partial] function to the received message is returned. @scala[The duration may
be left unspecified (empty parentheses are required in this case) to use
the deadline from the innermost enclosing @ref:[within](#testkit-within)
block instead.]
* @scala[`expectMsgClass[T](d: Duration, c: Class[T]): T`]@java[`public <T> T expectMsgClass(Duration max, Class<T> c)`]
   An object which is an instance of the given `Class` must be received
within the allotted time frame; the object will be returned. Note that this
does a conformance check; if you need the class to be equal, @scala[have a look at
`expectMsgAllClassOf` with a single given class argument]@java[you need to verify that afterwards].

@@@ div { .group-scala }

* `expectMsgType[T: Manifest](d: Duration)`
   An object which is an instance of the given type (after erasure) must be
received within the allotted time frame; the object will be returned. This
method is approximately equivalent to
`expectMsgClass(implicitly[ClassTag[T]].runtimeClass)`.

@@@

* @scala[`expectMsgAnyOf[T](d: Duration, obj: T*): T`]@java[`public Object expectMsgAnyOf(Duration max, Object... msg)`]
   An object must be received within the given time, and it must be equal (
compared with @scala[`==`]@java[`equals()`]) to at least one of the passed reference objects; the
received object will be returned.
* @scala[`expectMsgAnyClassOf[T](d: Duration, obj: Class[_ <: T]*): T`]@java[`public <T> T expectMsgAnyClassOf(Duration max, Class<? extends T>... c)`]
   An object must be received within the given time, and it must be an
instance of at least one of the supplied `Class` objects; the
received object will be returned. Note that this does a conformance check,
if you need the class to be equal you need to verify that afterwards.
* @scala[`expectMsgAllOf[T](d: Duration, obj: T*): Seq[T]`]@java[`public List<Object> expectMsgAllOf(Duration max, Object... msg)`]
   Several objects matching the size of the supplied object array must be
received within the given time, and for each of the given objects there
must exist at least one among the received ones which equals (compared with
@scala[`==`]@java[`equals()`]) it. The full sequence of received objects is returned in
the order received.

@@@ div { .group-scala }

* `expectMsgAllClassOf[T](d: Duration, c: Class[_ <: T]*): Seq[T]`
   Several objects matching the size of the supplied `Class` array
must be received within the given time, and for each of the given classes
there must exist at least one among the received objects whose class equals
(compared with `==`) it (this is *not* a conformance check). The full
sequence of received objects is returned.
* `expectMsgAllConformingOf[T](d: Duration, c: Class[_ <: T]*): Seq[T]`
   Several objects matching the size of the supplied `Class` array
must be received within the given time, and for each of the given classes
there must exist at least one among the received objects which is an
instance of this class. The full sequence of received objects is returned.

@@@

* @scala[`expectNoMessage(d: Duration)`]@java[`public void expectNoMessage(Duration max)`]
   No message must be received within the given time. This also fails if a
message has been received before calling this method which has not been
removed from the queue using one of the other methods.
* @scala[`receiveN(n: Int, d: Duration): Seq[AnyRef]`]@java[`List<Object> receiveN(int n, Duration max)`]
   `n` messages must be received within the given time; the received
messages are returned.

@@@ div { .group-scala }

* `fishForMessage(max: Duration, hint: String)(pf: PartialFunction[Any, Boolean]): Any`
   Keep receiving messages as long as the time is not used up and the partial
function matches and returns `false`. Returns the message received for
which it returned `true` or throws an exception, which will include the
provided hint for easier debugging.

@@@

In addition to message reception assertions there are also methods which help
with message flows:

@@@ div { .group-scala }

* `receiveOne(d: Duration): AnyRef` Tries to receive one message for at most the given time interval and
returns `null` in case of failure. If the given Duration is zero, the
call is non-blocking (polling mode).

@@@

* @scala[`receiveWhile[T](max: Duration, idle: Duration, messages: Int)(pf: PartialFunction[Any, T]): Seq[T]`]@java[`public <T> List<T> receiveWhile(Duration max, Duration idle, Int messages, Function<Object, T> f)`]
Collect messages as long as
    * they are matching the given partial function
    * the given time interval is not used up
    * the next message is received within the idle timeout
    * the number of messages has not yet reached the maximum
All collected messages are returned. @scala[The maximum duration defaults to the
time remaining in the innermost enclosing @ref:[within](#testkit-within)
block and the idle duration defaults to infinity (thereby disabling the
idle-timeout feature). The number of expected messages defaults to
`Int.MaxValue`, which effectively disables this limit.]

* @scala[`awaitCond(p: => Boolean, max: Duration, interval: Duration)`]@java[`public void awaitCond(Duration max, Duration interval, Supplier<Boolean> p)`]
   Poll the given condition every `interval` until it returns `true` or
the `max` duration is used up. @scala[The interval defaults to 100 ms and the
maximum defaults to the time remaining in the innermost enclosing
[within](#testkit-within) block.]

* @scala[`awaitAssert(a: => Any, max: Duration, interval: Duration)`]@java[`public void awaitAssert(Duration max, Duration interval, Supplier<Object> a)`]
Poll the given assert function every `interval` until it does not throw
an exception or the `max` duration is used up. If the timeout expires the
last exception is thrown. @scala[The interval defaults to 100 ms and the maximum defaults
to the time remaining in the innermost enclosing @ref:[within](#testkit-within)
block. The interval defaults to 100 ms and the maximum defaults to the time
remaining in the innermost enclosing @ref:[within](#testkit-within) block.] Return an arbitrary value that would be returned from awaitAssert if successful, if not interested in such value you can return null.

* @scala[`ignoreMsg(pf: PartialFunction[AnyRef, Boolean])`]@java[`public void ignoreMsg(Function<Object, Boolean> f)`]
@scala[`ignoreMsg`]@java[`public void ignoreMsg()`]
@java[There are also cases where not all messages sent to the test kit are actually
relevant to the test, but removing them would mean altering the actors under
test. For this purpose it is possible to ignore certain messages.]
@scala[The internal `testActor` contains a partial function for ignoring
messages: it will only enqueue messages which do not match the function or
for which the function returns `false`. This function can be set and
reset using the methods given above; each invocation replaces the previous
function, they are not composed.
   This feature is useful e.g. when testing a logging system, where you want
to ignore regular messages and are only interested in your specific ones.]

### Expecting Log Messages

Since an integration test does not allow observing the internal processing of the
participating actors, verifying expected exceptions cannot be done directly.
Instead, use the logging system for this purpose: replacing the normal event
handler with the `TestEventListener` and using an `EventFilter`
allows assertions on log messages, including those which are generated by
exceptions:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #event-filter }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-event-filter }

If the number of occurrences is specific—as demonstrated above—then `intercept`
will block until that number of matching messages have been received or the
timeout configured in `akka.test.filter-leeway` is used up (time starts
counting after the passed-in block of code returns). In case of a timeout the
test fails.

@@@ note

Be sure to exchange the default logger with the
`TestEventListener` in your `application.conf` to enable this
function:

```
akka.loggers = [akka.testkit.TestEventListener]
```

@@@

### Overriding behavior

Sometimes you want to 'hook into' your actor to be able to test some internals.
Usually, it is better to test an actors' external interface, but for example if
you want to test timing-sensitive behavior this can come in handy. Say for
instance you want to test an actor that schedules a task:

Scala
:  @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #timer }

Java
:  @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #timer }

You can override the method that does the scheduling in your test:

Scala
:  @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #timer-test }

Java
:  @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #timer-test }

<a id="testkit-within"></a>
### Timing Assertions

Another important part of functional testing concerns timing: certain events
must not happen immediately (like a timer), others need to happen before a
deadline. Therefore, all examination methods accept an upper time limit within
the positive or negative result must be obtained. Lower time limits need to be
checked external to the examination, which is facilitated by a new construct
for managing time constraints:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-within }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-within }

The block @scala[given to]@java[in] `within` must complete after a duration which
is between `min` and `max`, where the former defaults to zero. The
deadline calculated by adding the `max` parameter to the block's start
time is implicitly available within the block to all examination methods, if
you do not specify it, it is inherited from the innermost enclosing
`within` block.

It should be noted that if the last message-receiving assertion of the block is
`expectNoMessage` or `receiveWhile`, the final check of the
`within` is skipped to avoid false positives due to wake-up
latencies. This means that while individual contained assertions still use the
maximum time bound, the overall block may take arbitrarily longer in this case.


@@@ note

All times are measured using `System.nanoTime`, meaning that they describe
wall time, not CPU time or system time.

@@@

@@@ div { .group-scala }

Ray Roestenburg has written a great article on using the TestKit:
[https://web.archive.org/web/20180114133958/http://roestenburg.agilesquad.com/2011/02/unit-testing-akka-actors-with-testkit_12.html](https://web.archive.org/web/20180114133958/http://roestenburg.agilesquad.com/2011/02/unit-testing-akka-actors-with-testkit_12.html).
His full example is also available @ref:[here](testing.md#example).

@@@

#### Accounting for Slow Test Systems

The tight timeouts you use during testing on your lightning-fast notebook will
invariably lead to spurious test failures on the heavily loaded Jenkins server
(or similar). To account for this situation, all maximum durations are
internally scaled by a factor taken from the @ref:[Configuration](general/configuration-reference.md#config-akka-testkit),
`akka.test.timefactor`, which defaults to 1.

You can scale other durations with the same factor by using the @scala[implicit conversion
in `akka.testkit` package object to add dilated function to `Duration`]@java[`dilated` method in `TestKit`].

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #duration-dilation }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #duration-dilation }

@@@ div { .group-scala }

### Resolving Conflicts with Implicit ActorRef

If you want the sender of messages inside your TestKit-based tests to be the `testActor`
 mix in `ImplicitSender` into your test.

@@snip [PlainWordSpec.scala](/akka-docs/src/test/scala/docs/testkit/PlainWordSpec.scala) { #implicit-sender }

@@@

### Using Multiple Probe Actors

When the actors under test are supposed to send various messages to different
destinations, it may be difficult distinguishing the message streams arriving
at the `testActor` when using the `TestKit` as @scala[a mixin]@java[shown until now]. Another
approach is to use it for the creation of simple probe actors to be inserted in the
message flows. @scala[To make this more powerful and convenient, there is a concrete
implementation called `TestProbe`.] The functionality is best explained
using a small example:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #imports-test-probe }
@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #my-double-echo }
@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-probe }

@scala[Here the system under test is simulated by `MyDoubleEcho`, which is
supposed to mirror its input to two outputs. Attaching two test probes enables
verification of the (simplistic) behavior]@java[This simple test verifies an equally simple Forwarder actor by injecting a
probe as the forwarder’s target]. Another example would be two actors
A and B which collaborate by A sending messages to B. To verify this
message flow, a `TestProbe` could be inserted as a target of A, using the
forwarding capabilities or auto-pilot described below to include a real B in
the test setup.

If you have many test probes, you can name them to get meaningful actor names
in test logs and assertions:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe-with-custom-name }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-probe-with-custom-name }

Probes may also be equipped with custom assertions to make your test code even
more concise and clear:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-special-probe }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-special-probe }

You have complete flexibility here in mixing and matching the `TestKit`
facilities with your checks and choosing an intuitive name for it. In real
life your code will probably be a bit more complicated than the example given
above; just use the power!

@@@ warning

Any message sent from a `TestProbe` to another actor which runs on the
CallingThreadDispatcher runs the risk of dead-lock if that other actor might
also send to this probe. The implementation of `TestProbe.watch` and
`TestProbe.unwatch` will also send a message to the actor being watched, which
means that it is dangerous to try watching e.g. `TestActorRef` from a
`TestProbe`.

@@@

#### Watching Other Actors from Probes

A @scala[`TestProbe`]@java[`TestKit`] can register itself for DeathWatch of any other actor:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe-watch }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-probe-watch }

#### Replying to Messages Received by Probes

@scala[The probes keep track of the communications channel for replies, if possible,
so they can also reply]@java[The probe stores the sender of the last dequeued message (i.e. after its
`expectMsg*` reception), which may be retrieved using the
`getLastSender()` method. This information can also implicitly be used
for having the probe reply to the last received message]:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe-reply }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-probe-reply }

#### Forwarding Messages Received by Probes

@scala[Given a destination actor `dest` which in the nominal actor network would
receive a message from actor `source`. If you arrange for the message to be
sent to a `TestProbe` `probe` instead, you can make assertions
concerning volume and timing of the message flow while still keeping the
network functioning]@java[The probe can also forward a received message (i.e. after its `expectMsg*`
reception), retaining the original sender]:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe-forward-actors }
@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-probe-forward }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-probe-forward }

@scala[The `dest` actor will receive the same message invocation as if no test probe
had intervened.]

#### Auto-Pilot

Receiving messages in a queue for later inspection is nice, but to
keep a test running and verify traces later you can also install an
`AutoPilot` in the participating test probes (actually in any
`TestKit`) which is invoked before enqueueing to the inspection queue.
This code can be used to forward messages, e.g. in a chain `A --> Probe -->
B`, as long as a certain protocol is obeyed.

Scala
:   @@snip [TestProbeSpec.scala](/akka-testkit/src/test/scala/akka/testkit/TestProbeSpec.scala) { #autopilot }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-auto-pilot }

The `run` method must return the auto-pilot for the next message, @scala[which
can be `KeepRunning` to retain the current one or `NoAutoPilot`
to switch it off]@java[wrapped
in an `Option`; setting it to `None` terminates the auto-pilot].

#### Caution about Timing Assertions

The behavior of `within` blocks when using test probes might be perceived
as counter-intuitive: you need to remember that the nicely scoped deadline as
described @ref:[above](#testkit-within) is local to each probe. Hence, probes
do not react to each other's deadlines or the deadline set in an enclosing
`TestKit` instance:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-within-probe }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-within-probe }

Here, the @scala[`expectMsg`]@java[`expectMsgEquals`] call will use the default timeout.

### Testing parent-child relationships

The parent of an actor is always the actor that created it. At times this leads to
a coupling between the two that may not be straightforward to test.
There are several approaches to improve the testability of a child actor that
needs to refer to its parent:

 1. when creating a child, pass an explicit reference to its parent
 2. create the child with a `TestProbe` as parent
 3. create a fabricated parent when testing

Conversely, a parent's binding to its child can be lessened as follows:

 4. when creating a parent, tell the parent how to create its child

For example, the structure of the code you want to test may follow this pattern:

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #test-example }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-example }

#### Introduce a child to its parent

The first option is to avoid the use of the `context.parent` function and create
a child with a custom parent by passing an explicit reference to its parent instead.

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #test-dependentchild }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-dependentchild }

#### Create the child using @scala[TestProbe]@java[TestKit]

The @scala[`TestProbe`]@java[`TestKit`] class can create actors that will run with the test probe as a parent.
This will cause any messages the child actor sends to @scala[*context.parent*]@java[*getContext().getParent()*] to
end up in the test probe.

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #test-TestProbe-parent }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-TestProbe-parent }

#### Using a fabricated parent

If you prefer to avoid modifying the parent or child constructor you can
create a fabricated parent in your test. This, however, does not enable you to test
the parent actor in isolation.

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #test-fabricated-parent }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-fabricated-parent-creator }
@@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-fabricated-parent }

#### Externalize child making from the parent

Alternatively, you can tell the parent how to create its child. There are two ways
to do this: by giving it a `Props` object or by giving it a function which takes care of creating the child actor:

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #test-dependentparent }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-dependentparent }
@@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #test-dependentparent-generic }


Creating the @scala[`Props`]@java[`Actor`] is straightforward and the function may look like this in your test code:

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #child-maker-test }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #child-maker-test }

And like this in your application code:

Scala
:   @@snip [ParentChildSpec.scala](/akka-docs/src/test/scala/docs/testkit/ParentChildSpec.scala) { #child-maker-prod }

Java
:   @@snip [ParentChildTest.java](/akka-docs/src/test/java/jdocs/testkit/ParentChildTest.java) { #child-maker-prod }

Which of these methods is the best depends on what is most important to test. The
most generic option is to create the parent actor by passing it a function that is
responsible for the Actor creation, but @scala[the]@java[using `TestProbe` or having a] fabricated parent is often sufficient.

## CallingThreadDispatcher

The `CallingThreadDispatcher` runs invocations on the current thread only. This
dispatcher does not create any new threads.

It is possible to use the `CallingThreadDispatcher` in unit testing, as
described above, but originally it was conceived to allow uninterrupted
stack traces to be generated in case of an error. As this special dispatcher
runs everything which would normally be queued directly on the current thread,
the full history of a message's processing chain is recorded on the call stack,
so long as all intervening actors run on this dispatcher.

### How to use it

Just set the dispatcher as you normally would:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #calling-thread-dispatcher }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #calling-thread-dispatcher }

### How it works

When receiving an invocation, the `CallingThreadDispatcher` checks
whether the receiving actor is already active on the current thread. The
simplest example of this situation is an actor which sends a message to
itself. In this case, processing cannot continue immediately as that would
violate the actor model, so the invocation is queued and will be processed when
the active invocation on that actor finishes its processing; thus, it will be
processed on the calling thread, but after the actor finishes its
previous work. In the other case, the invocation is processed
immediately on the current thread. Futures scheduled via this dispatcher are
also executed immediately.

This scheme makes the `CallingThreadDispatcher` work like a general
purpose dispatcher for any actors which never block on external events.

In the presence of multiple threads, it may happen that two invocations of an
actor running on this dispatcher happen on two different threads at the same
time. In this case, both will be processed directly on their respective
threads, where both compete for the actor's lock and the loser has to wait.
Thus, the actor model is left intact, but the price is the loss of concurrency due
to limited scheduling. In a sense, this is equivalent to traditional mutex style
concurrency.

The other remaining difficulty is correct handling of suspend and resume: when
an actor is suspended, subsequent invocations will be queued in thread-local
queues (the same ones used for queuing in the normal case). The call to
`resume`, however, is done by one specific thread, and all other threads
in the system will probably not be executing this specific actor, which leads
to the problem that the thread-local queues cannot be emptied by their native
threads. Hence, the thread calling `resume` will collect all currently
queued invocations from all threads into its queue and process them.

### Limitations

@@@ warning

In case the CallingThreadDispatcher is used for top-level actors,
without going through TestActorRef, then there is a time window during which
the actor is awaiting construction by the user guardian actor. Sending
messages to the actor during this time will result in them being
enqueued and then executed on the guardian’s thread instead of the caller’s
thread. To avoid this, use TestActorRef.

@@@

If an actor's behavior blocks on something which would normally be affected
by the calling actor after having sent the message, this will dead-lock when using this dispatcher. This is a common scenario in actor tests
based on `CountDownLatch` for synchronization:

```scala
val latch = new CountDownLatch(1)
actor ! startWorkAfter(latch)   // actor will call latch.await() before proceeding
doSomeSetupStuff()
latch.countDown()
```

The example would hang indefinitely within the message processing initiated on
the second line and never reach the fourth line, which would unblock it on a
normal dispatcher.

Thus, keep in mind that the `CallingThreadDispatcher` is not a
general-purpose replacement for the normal dispatchers. If you are looking 
for a tool to help you debug dead-locks,
the `CallingThreadDispatcher` may help with some error
scenarios, but keep in mind that it may give false negatives as well as
false positives.

### Thread Interruptions

If the CallingThreadDispatcher sees that the current thread has its
`isInterrupted()` flag set when message processing returns, it will throw an
`InterruptedException` after finishing all its processing (i.e. all
messages which need processing as described above are processed before this
happens). As `tell` cannot throw exceptions due to its contract, this
exception will then be caught and logged, and the thread’s interrupted status
will be set again.

If during message processing an `InterruptedException` is thrown then it
will be caught inside the CallingThreadDispatcher’s message handling loop, the
thread’s interrupted flag will be set and processing continues normally.

@@@ note

In summary, if the current thread is
interrupted while doing work under the CallingThreadDispatcher, then that
will result in the `isInterrupted` flag to be `true` when the message
send returns and no `InterruptedException` will be thrown.

@@@

### Benefits

To summarize, these are the features that `CallingThreadDispatcher`
has to offer:

 * Deterministic execution of single-threaded tests while retaining nearly full
actor semantics
 * Full message processing history leading up to the point of failure in
exception stack traces
 * Exclusion of certain classes of dead-lock scenarios

<a id="actor-logging"></a>
## Tracing Actor Invocations

The testing facilities described up to this point were aiming at formulating
assertions about a system’s behavior. If a test fails, it is usually your job
to find the cause, fix it and verify the test again. This process is supported
by debuggers as well as logging, where the Akka toolkit offers the following
options:

* *Logging of exceptions thrown within Actor instances*
   It is always on; in contrast to the other logging mechanisms, this logs at
`ERROR` level.

@@@ div { .group-scala }
* *Logging of message invocations on certain actors*
   This is enabled by a setting in the @ref:[Configuration](general/configuration-reference.md#config-akka-actor) — namely
`akka.actor.debug.receive` — which enables the `loggable`
statement to be applied to an actor’s `receive` function:

@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #logging-receive }

If the aforementioned setting is not given in the @ref:[Configuration](general/configuration-reference.md#config-akka-actor), this method will
pass through the given `Receive` function unmodified, meaning that
there is no runtime cost unless enabled.

The logging feature is coupled to this specific local mark-up because
enabling it uniformly on all actors is not usually what you need, and it
would lead to endless loops if it were applied to event bus logger listeners.

@@@

* *Logging of special messages*
   Actors handle certain special messages automatically, e.g. `Kill`,
`PoisonPill`, etc. Tracing of these message invocations is enabled by
the setting `akka.actor.debug.autoreceive`, which enables this on all
actors.
* *Logging of the actor lifecycle*
   Actor creation, start, restart, monitor start, monitor stop and stop may be traced by
enabling the setting `akka.actor.debug.lifecycle`; this, too, is enabled
uniformly on all actors.

Logging of these messages is at `DEBUG` level. To summarize, you can enable
full logging of actor activities using this configuration fragment:

```
akka {
  loglevel = "DEBUG"
  actor {
    debug {
      receive = on
      autoreceive = on
      lifecycle = on
    }
  }
}
```

@@@ div { .group-scala }

## Different Testing Frameworks

Akka’s test suite is written using [ScalaTest](https://www.scalatest.org),
which also shines through in documentation examples. However, the TestKit and
its facilities do not depend on that framework, so you can essentially use
whichever suits your development style best.

This section contains a collection of known gotchas with some other frameworks,
which is by no means exhaustive and does not imply an endorsement or special
support.

### When you need it to be a trait

If for some reason it is a problem to inherit from `TestKit` due to it
being a concrete class instead of a trait, there’s `TestKitBase`:

@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-kit-base }

The `implicit lazy val system` must be declared exactly like that (you can of
course pass arguments to the actor system factory as needed) because trait
`TestKitBase` needs the system during its construction.

Warning: use of the trait is discouraged because of potential issues with binary
backwards compatibility in the future, use at own risk.

### Specs2

Some [Specs2](https://etorreborre.github.io/specs2/) users have contributed examples of how to work around some clashes which may arise:

 * Mixing TestKit into `org.specs2.mutable.Specification` results in a
 name clash involving the `end` method (which is a private variable in
 TestKit and an abstract method in Specification); if mixing in TestKit first,
 the code may compile but might then fail at runtime. The workaround—which is
 beneficial also for the third point—is to apply the TestKit together
 with `org.specs2.specification.Scope`.
  * The Specification traits provide a `Duration` DSL which uses partly the same method names as `scala.concurrent.duration.Duration`, resulting in ambiguous implicits if `scala.concurrent.duration._` is imported. There are two workarounds:
     * either use the Specification variant of Duration and supply an implicit conversion to the Akka Duration. This conversion is not supplied with the
 Akka distribution because that would mean that our JAR files would depend on
 Specs2, which is not justified by this little feature.
     * or mix `org.specs2.time.NoTimeConversions` into the Specification.
  * Specifications are by default executed concurrently, which requires some care when writing the tests or the `sequential` keyword.

@@@

## Configuration

There are several configuration properties for the TestKit module, please refer
to the @ref:[reference configuration](general/configuration-reference.md#config-akka-testkit).

@@@ div { .group-scala }

## Example

Ray Roestenburg's example code from his blog, which unfortunately is only available on
[web archive](https://web.archive.org/web/20180114133958/http://roestenburg.agilesquad.com/2011/02/unit-testing-akka-actors-with-testkit_12.html),
adapted to work with Akka 2.x.

@@snip [TestKitUsageSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestKitUsageSpec.scala) { #testkit-usage }

@@@

<a id="sync-testing"></a>
## Synchronous Testing: `TestActorRef`

Testing the business logic inside `Actor` classes can be divided into
two parts: first, each atomic operation must work in isolation, then sequences
of incoming events must be processed correctly, even in the presence of some
possible variability in the ordering of events. The former is the primary use
case for single-threaded unit testing, while the latter can only be verified in
integration tests.

Normally, the `ActorRef` shields the underlying `Actor` instance
from the outside, the only communications channel is the actor's mailbox. This
restriction impedes unit testing, which led to the inception of the
`TestActorRef`. This special type of reference is designed specifically
for test purposes and allows access to the actor in two ways: either by
obtaining a reference to the underlying actor instance or by invoking or
querying the actor's behavior (`receive`). Each one warrants its section below.

@@@ note

It is highly recommended to stick to traditional behavioral testing (using messaging
to ask the Actor to reply with the state you want to run assertions against),
instead of using `TestActorRef` whenever possible.

@@@

@@@ warning

Due to the synchronous nature of `TestActorRef`, it will **not** work with some support
traits that Akka provides as they require asynchronous behaviors to function properly.
Examples of traits that do not mix well with test actor refs are @ref:[PersistentActor](persistence.md#example)
and @ref:[AtLeastOnceDelivery](persistence.md#at-least-once-delivery) provided by @ref:[Akka Persistence](persistence.md).

@@@

### Obtaining a Reference to an `Actor`

Having access to the actual `Actor` object allows the application of all
traditional unit testing techniques on the contained methods. Obtaining a
reference is done like this:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-actor-ref }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-actor-ref }

Since `TestActorRef` is generic in the actor type it returns the
underlying actor with its proper static type. From this point on you may bring
any unit testing tool to bear on your actor as usual.

@@@ div { .group-scala }


<a id="testfsmref"></a>
### Testing Finite State Machines

If your actor under test is an `FSM`, you may use the special
`TestFSMRef` which offers all features of a normal `TestActorRef`
and besides allows access to the internal state:

@@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-fsm-ref }

Due to a limitation in Scala’s type inference, there is only the factory method
shown above, so you will probably write code like `TestFSMRef(new MyFSM)`
instead of the hypothetical `ActorRef`-inspired `TestFSMRef[MyFSM]`.
All methods shown above directly access the FSM state without any
synchronization; this is perfectly alright if the `CallingThreadDispatcher`
is used and no other threads are involved, but it may lead to surprises if you
were to exercise timer events, because those are executed on the
`Scheduler` thread.

@@@

### Testing the Actor's Behavior

When the dispatcher invokes the processing behavior of an actor on a message,
it calls `apply` on the current behavior registered for the
actor. This starts with the return value of the declared `receive`
method, but it may also be changed using `become` and `unbecome` in
response to external messages. All of this contributes to the overall actor
behavior and it does not lend itself to easy testing on the `Actor`
itself. Therefore the `TestActorRef` offers a different mode of
operation to complement the `Actor` testing: it supports all operations
also valid on normal `ActorRef`. Messages sent to the actor are
processed synchronously on the current thread and answers may be sent back as
usual. This trick is made possible by the `CallingThreadDispatcher`
described below (see @ref:[CallingThreadDispatcher](#callingthreaddispatcher)); this dispatcher is set
implicitly for any actor instantiated into a `TestActorRef`.

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-behavior }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-behavior }

As the `TestActorRef` is a subclass of `LocalActorRef` with a few
special extras, also aspects like supervision and restarting work properly, but
beware that execution is only strictly synchronous as long as all actors
involved use the `CallingThreadDispatcher`. As soon as you add elements
which include more sophisticated scheduling you leave the realm of unit testing
as you then need to think about asynchronicity again (in most cases the problem
will be to wait until the desired effect had a chance to happen).

One more special aspect which is overridden for single-threaded tests is the
`receiveTimeout`, as including that would entail asynchronous queuing of
`ReceiveTimeout` messages, violating the synchronous contract.

@@@ note

To summarize: `TestActorRef` overwrites two fields: it sets the
dispatcher to `CallingThreadDispatcher.global` and it sets the
`receiveTimeout` to None.

@@@

### The Way In-Between: Expecting Exceptions

If you want to test the actor behavior, including hot swapping, but without
involving a dispatcher and without having the `TestActorRef` swallow
any thrown exceptions, then there is another mode available for you: use
the `receive` method on `TestActorRef`, which will be forwarded to the
underlying actor:

Scala
:   @@snip [TestkitDocSpec.scala](/akka-docs/src/test/scala/docs/testkit/TestkitDocSpec.scala) { #test-expecting-exceptions }

Java
:   @@snip [TestKitDocTest.java](/akka-docs/src/test/java/jdocs/testkit/TestKitDocTest.java) { #test-expecting-exceptions }

### Use Cases

You may mix and match both modi operandi of `TestActorRef` as
suits your test needs:

 * one common use case is setting up the actor into a specific internal state
before sending the test message
 * another is to verify correct internal state transitions after having sent
the test message

Feel free to experiment with the possibilities, and if you find useful
patterns, don't hesitate to let the Akka forums know about them! Who knows,
common operations might even be worked into nice DSLs.
