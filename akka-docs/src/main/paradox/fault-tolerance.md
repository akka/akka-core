# Classic Fault Tolerance

@@include[includes.md](includes.md) { #actor-api }
For the full documentation of this feature and for new projects see @ref:[fault tolerance](typed/fault-tolerance.md).

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

The concept of fault tolerance relates to actors, so in order to use these make sure to depend on actors.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

As explained in @ref:[Actor Systems](general/actor-systems.md) each actor is the supervisor of its
children, and as such each actor defines fault handling supervisor strategy.
This strategy cannot be changed afterwards as it is an integral part of the
actor system’s structure.

## Fault Handling in Practice

First, let us look at a sample that illustrates one way to handle data store errors,
which is a typical source of failure in real world applications. Of course it depends
on the actual application what is possible to do when the data store is unavailable,
but in this sample we use a best effort re-connect approach.

Read the following source code. The inlined comments explain the different pieces of
the fault handling and why they are added. It is also highly recommended to run this
sample as it is easy to follow the log output to understand what is happening at runtime.

## Creating a Supervisor Strategy

The following sections explain the fault handling mechanism and alternatives
in more depth.

For the sake of demonstration let us consider the following strategy:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #strategy }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #strategy }

We have chosen a few well-known exception types in order to demonstrate the
application of the fault handling directives described in @ref:[supervision](general/supervision.md).
First off, it is a one-for-one strategy, meaning that each child is treated
separately (an all-for-one strategy works very similarly, the only difference
is that any decision is applied to all children of the supervisor, not only the
failing one). 
In the above example, `10` and @scala[`1 minute`]@java[`Duration.create(1, TimeUnit.MINUTES)`] are passed to the `maxNrOfRetries`
and `withinTimeRange` parameters respectively, which means that the strategy restarts a child up to 10 restarts per minute.
The child actor is stopped if the restart count exceeds `maxNrOfRetries` during the `withinTimeRange` duration.

Also, there are special values for these parameters. If you specify:

* `-1` to `maxNrOfRetries`, and @scala[`Duration.Inf`]@java[`Duration.Inf()`] to `withinTimeRange`
    * then the child is always restarted without any limit
* `-1` to `maxNrOfRetries`, and a non-infinite `Duration` to `withinTimeRange` 
    * `maxNrOfRetries` is treated as `1`
* a non-negative number to `maxNrOfRetries` and @scala[`Duration.Inf`]@java[`Duration.Inf()`] to `withinTimeRange`
    * `withinTimeRange` is treated as infinite duration (i.e.) no matter how long it takes, once the restart count exceeds `maxNrOfRetries`, the child actor is stopped  
   
The match statement which forms the bulk of the body   
@scala[is of type `Decider` which is a `PartialFunction[Throwable, Directive]`.]
@java[consists of `PFBuilder` returned by `DeciderBuilder`'s `match` method, where the builder is finished by the `build` method.]
This is the piece which maps child failure types to their corresponding directives.

@@@ note

If the strategy is declared inside the supervising actor (as opposed to
@scala[within a companion object]@java[a separate class]) its decider has access to all internal state of
the actor in a thread-safe fashion, including obtaining a reference to the
currently failed child (available as the @scala[`sender`]@java[`getSender()`] of the failure message).

@@@

### Default Supervisor Strategy

`Escalate` is used if the defined strategy doesn't cover the exception that was thrown.

When the supervisor strategy is not defined for an actor the following
exceptions are handled by default:

 * `ActorInitializationException` will stop the failing child actor
 * `ActorKilledException` will stop the failing child actor
 * `DeathPactException` will stop the failing child actor
 * `Exception` will restart the failing child actor
 * Other types of `Throwable` will be escalated to parent actor

If the exception escalate all the way up to the root guardian it will handle it
in the same way as the default strategy defined above.

@@@ div { .group-scala }

You can combine your own strategy with the default strategy:

@@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #default-strategy-fallback }

@@@

### Stopping Supervisor Strategy

Closer to the Erlang way is the strategy to stop children when they fail
and then take corrective action in the supervisor when DeathWatch signals the
loss of the child. This strategy is also provided pre-packaged as
`SupervisorStrategy.stoppingStrategy` with an accompanying
`StoppingSupervisorStrategy` configurator to be used when you want the
`"/user"` guardian to apply it.

### Logging of Actor Failures

By default the `SupervisorStrategy` logs failures unless they are escalated.
Escalated failures are supposed to be handled, and potentially logged, at a level
higher in the hierarchy.

Log levels can be controlled by providing a `Decider` and using the appropriate decision methods accepting a `LogLevel` on `SupervisorStrategy`.

You can mute the default logging of a `SupervisorStrategy` by setting
`loggingEnabled` to `false` when instantiating it. Customized logging
can be done inside the `Decider`. Note that the reference to the currently
failed child is available as the `sender` when the `SupervisorStrategy` is
declared inside the supervising actor.

You may also customize the logging in your own `SupervisorStrategy` implementation
by overriding the `logFailure` method.

## Supervision of Top-Level Actors

Toplevel actors means those which are created using `system.actorOf()`, and
they are children of the @ref:[User Guardian](supervision-classic.md#user-guardian). There are no
special rules applied in this case, the guardian applies the configured
strategy.

## Test Application

The following section shows the effects of the different directives in practice,
where a test setup is needed. First off, we need a suitable supervisor:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #supervisor }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #supervisor }

This supervisor will be used to create a child, with which we can experiment:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #child }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #child }

The test is easier by using the utilities described in @scala[@ref:[Testing Actor Systems](testing.md)]@java[@ref:[TestKit](testing.md)],
where `TestProbe` provides an actor ref useful for receiving and inspecting replies.

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #testkit }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #testkit }

Let us create actors:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #create }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #create }

The first test shall demonstrate the `Resume` directive, so we try it out by
setting some non-initial state in the actor and have it fail:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #resume }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #resume }

As you can see the value 42 survives the fault handling directive. Now, if we
change the failure to a more serious `NullPointerException`, that will no
longer be the case:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #restart }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #restart }

And finally in case of the fatal `IllegalArgumentException` the child will be
terminated by the supervisor:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #stop }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #stop }

Up to now the supervisor was completely unaffected by the child’s failure,
because the directives set did handle it. In case of an `Exception`, this is not
true anymore and the supervisor escalates the failure.

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #escalate-kill }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #escalate-kill }

The supervisor itself is supervised by the top-level actor provided by the
`ActorSystem`, which has the default policy to restart in case of all
`Exception` cases (with the notable exceptions of
`ActorInitializationException` and `ActorKilledException`). Since the
default directive in case of a restart is to kill all children, we expected our poor
child not to survive this failure.

In case this is not desired (which depends on the use case), we need to use a
different supervisor which overrides this behavior.

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #supervisor2 }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #supervisor2 }

With this parent, the child survives the escalated restart, as demonstrated in
the last test:

Scala
:  @@snip [FaultHandlingDocSpec.scala](/akka-docs/src/test/scala/docs/actor/FaultHandlingDocSpec.scala) { #escalate-restart }

Java
:  @@snip [FaultHandlingTest.java](/akka-docs/src/test/java/jdocs/actor/FaultHandlingTest.java) { #escalate-restart }


## Delayed restarts for classic actors

The supervision strategy to restart a classic actor only provides immediate restart. In some cases that will only trigger
the same failure right away and giving things a bit of time before restarting is required to actually resolve the failure.

The `akka.pattern.BackoffSupervisor` implements the so-called
*exponential backoff supervision strategy*, starting a child actor again when it fails, each time with a growing time delay between restarts.

This pattern is useful when the started actor fails <a id="^1" href="#1">[1]</a> because some external resource is not available,
and we need to give it some time to start-up again. One of the prime examples when this is useful is
when a @ref:[PersistentActor](persistence.md) fails (by stopping) with a persistence failure - which indicates that
the database may be down or overloaded, in such situations it makes most sense to give it a little bit of time
to recover before the persistent actor is started.

> <a id="1" href="#^1">[1]</a> A failure can be indicated in two different ways; by an actor stopping or crashing.

### Supervision strategies

There are two basic supervision strategies available for backoff:

* 'On failure': The supervisor will terminate and then start the supervised actor if it crashes. If the supervised actor stops normally (e.g. through `context.stop`), the supervisor will be terminated and no further attempt to start the supervised actor will be done.
* 'On stop': The supervisor will terminate and then start the supervised actor if it terminates in any way (consider this for `PersistentActor` since they stop on persistence failures instead of crashing)

To note that this supervision strategy does not restart the actor but rather stops and starts it. Be aware of it if you 
use @scala[`Stash` trait’s] @java[`AbstractActorWithStash`] in combination with the backoff supervision strategy.
The `preRestart` hook will not be executed if the supervised actor fails or stops and you will miss the opportunity
to unstash the messages.

### Sharding
If the 'on stop' strategy is used for sharded actors a final termination message should be configured and used to terminate the actor on passivation. Otherwise the supervisor will just stop and start the actor again.

The termination message is configured with:

@@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-sharded }

And must be used for passivation:

@@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-sharded-passivation }


### Simple backoff

The following snippet shows how to create a backoff supervisor which will start the given echo actor after it has stopped
because of a failure, in increasing intervals of 3, 6, 12, 24 and finally 30 seconds:

Scala
:  @@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-stop }

Java
:  @@snip [BackoffSupervisorDocTest.java](/akka-docs/src/test/java/jdocs/pattern/BackoffSupervisorDocTest.java) { #backoff-stop }

Using a `randomFactor` to add a little bit of additional variance to the backoff intervals
is highly recommended, in order to avoid multiple actors re-start at the exact same point in time,
for example because they were stopped due to a shared resource such as a database going down
and re-starting after the same configured interval. By adding additional randomness to the
re-start intervals the actors will start in slightly different points in time, thus avoiding
large spikes of traffic hitting the recovering shared database or other resource that they all need to contact.

The `akka.pattern.BackoffSupervisor` actor can also be configured to stop and start the actor after a delay when the actor 
crashes and the supervision strategy decides that it should restart.

The following snippet shows how to create a backoff supervisor which will start the given echo actor after it has crashed
because of some exception, in increasing intervals of 3, 6, 12, 24 and finally 30 seconds:

Scala
:  @@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-fail }

Java
:  @@snip [BackoffSupervisorDocTest.java](/akka-docs/src/test/java/jdocs/pattern/BackoffSupervisorDocTest.java) { #backoff-fail }

### Customization

The `akka.pattern.BackoffOnFailureOptions` and `akka.pattern.BackoffOnRestartOptions` can be used to customize the behavior of the back-off supervisor actor.
Options are:
* `withAutoReset`: The backoff is reset if no failure/stop occurs within the duration. This is the default behaviour with `minBackoff` as default value
* `withManualReset`: The child must send `BackoffSupervisor.Reset` to its backoff supervisor (parent)
* `withSupervisionStrategy`: Sets a custom `OneForOneStrategy` (as each backoff supervisor only has one child). The default strategy uses the `akka.actor.SupervisorStrategy.defaultDecider` which stops and starts the child on exceptions.
* `withMaxNrOfRetries`: Sets the maximum number of retries until the supervisor will give up (`-1` is default which means no limit of retries). Note: This is set on the supervision strategy, so setting a different strategy resets the `maxNrOfRetries`.
* `withReplyWhileStopped`: By default all messages received while the child is stopped are forwarded to dead letters. With this set, the supervisor will reply to the sender instead.

Only available on `BackoffOnStopOptions`:
* `withDefaultStoppingStrategy`: Sets a `OneForOneStrategy` with the stopping decider that stops the child on all exceptions.
* `withFinalStopMessage`: Allows to define a predicate to decide on finally stopping the child (and supervisor). Used for passivate sharded actors - see above.

Some examples:

@@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-custom-stop }

The above code sets up a back-off supervisor that requires the child actor to send a `akka.pattern.BackoffSupervisor.Reset` message
to its parent when a message is successfully processed, resetting the back-off. It also uses a default stopping strategy, any exception
will cause the child to stop.

@@snip [BackoffSupervisorDocSpec.scala](/akka-docs/src/test/scala/docs/pattern/BackoffSupervisorDocSpec.scala) { #backoff-custom-fail }

The above code sets up a back-off supervisor that stops and starts the child after back-off if MyException is thrown, any other exception will be
escalated. The back-off is automatically reset if the child does not throw any errors within 10 seconds.
