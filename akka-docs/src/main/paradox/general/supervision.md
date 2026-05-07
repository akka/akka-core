---
project.description: Hierarchical supervision, lifecycle monitoring and error or failure handling in Akka.
---
# Supervision and Monitoring

This chapter outlines the concept behind supervision, the primitives offered
and their semantics. For details on how that translates into real code, please
refer to @ref:[supervision](../typed/fault-tolerance.md).

Supervision has changed since classic, for details on classic supervision see @ref:[Classic Supervision](../supervision-classic.md)

<a id="supervision-directives"></a>
## What Supervision Means

There are two categories of exception that can happen in an actor:

 1. Input validation errors, expected exceptions which can be handled with a regular try-catch 
    or other language and standard library tools.
 1. Unexpected **failures**, for example a network resource being unavailable, a disk write failing or perhaps
    a bug in the application logic.

Supervision deals with failures and should be separated from the business logic while validating data and handling
of expected exceptions is a vital part of the business logic. Therefore supervision is added to an actor as decoration
rather than something that is intermingled with the message processing logic of the actor.

Depending on the nature of the work to be supervised and the nature of the failure, supervision
provides the following three strategies:

 1. Resume the actor, keeping its accumulated internal state
 2. Restart the actor, clearing out its accumulated internal state, with a potential delay starting again
 3. Stop the actor permanently

Since actors are part of a hierarchy it can often make sense to propagate
the permanent failures upwards, if all children of an actor has stopped
unexpectedly it may make sense for the actor itself to restart or stop to
get back to a functional state. This can be achieved through a combination of
supervision and watching the children to get notified when they terminate.
An example of this can be found in @ref:[Bubble failures up through the hierarchy](../typed/fault-tolerance.md#bubble).

## The Top-Level actors

An actor system will during its creation start at least two actors. 

### `/user`: the user guardian actor

This is the top level user provided actor, meant to bootstrap the application
by spawning subsystems as children. When the user guardian stops the entire
actor system is shut down.

### `/system`: the system guardian actor

This special guardian has been introduced in order to achieve an orderly
shut-down sequence where logging remains active while all normal actors
terminate, even though logging itself is implemented using actors. This is
realized by having the system guardian watch the user guardian and initiate its own
shut-down upon having seen the user guardian stop. 

<a id="supervision-restart"></a>
## What Restarting Means

When presented with an actor which failed while processing a certain message,
causes for the failure fall into three categories:

 * Systematic (i.e. programming) error for the specific message received
 * (Transient) failure of some external resource used during processing the message
 * Corrupt internal state of the actor

Unless the failure is specifically recognizable, the third cause cannot be
ruled out, which leads to the conclusion that the internal state needs to be
cleared out. If the supervisor decides that its other children or itself is not
affected by the corruption—e.g. because of conscious application of the error
kernel pattern—it is therefore best to restart the actor. This is carried out
by creating a new instance of the underlying @apidoc[Behavior] class and replacing
the failed instance with the fresh one inside the child's @apidoc[typed.ActorRef];
the ability to do this is one of the reasons for encapsulating actors within
special references. The new actor then resumes processing its mailbox, meaning
that the restart is not visible outside of the actor itself with the notable
exception that the message during which the failure occurred is not
re-processed.

## What Lifecycle Monitoring Means

@@@ note

Lifecycle Monitoring in Akka is usually referred to as `DeathWatch`

@@@

In contrast to the special relationship between parent and child described
above, each actor may monitor any other actor. Since actors emerge from
creation fully alive and restarts are not visible outside of the affected
supervisors, the only state change available for monitoring is the transition
from alive to dead. Monitoring is thus used to tie one actor to another so that
it may react to the other actor’s termination, in contrast to supervision which
reacts to failure.

Lifecycle monitoring is implemented using a @apidoc[typed.Terminated] message to be
received by the monitoring actor, where the default behavior is to throw a
special @apidoc[typed.DeathPactException] if not otherwise handled. In order to start
listening for @apidoc[typed.Terminated] messages, invoke
@apidoc[ActorContext.watch(targetActorRef)](typed.*.ActorContext) {scala="#watch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#watch(akka.actor.typed.ActorRef)"}.  To stop listening, invoke
@apidoc[ActorContext.unwatch(targetActorRef)](typed.*.ActorContext) {scala="#unwatch[U](other:akka.actor.typed.ActorRef[U]):Unit" java="#unwatch(akka.actor.typed.ActorRef)"}.  One important property is that the
message will be delivered irrespective of the order in which the monitoring
request and target’s termination occur, i.e. you still get the message even if
at the time of registration the target is already dead.

## Actors and exceptions

It can happen that while a message is being processed by an actor, that some
kind of exception is thrown, e.g. a database exception.

### What happens to the Message

If an exception is thrown while a message is being processed (i.e. taken out of
its mailbox and handed over to the current behavior), then this message will be
lost. It is important to understand that it is not put back on the mailbox. So
if you want to retry processing of a message, you need to deal with it yourself
by catching the exception and retry your flow. Make sure that you put a bound
on the number of retries since you don't want a system to livelock (so
consuming a lot of cpu cycles without making progress).

### What happens to the mailbox

If an exception is thrown while a message is being processed, nothing happens to
the mailbox. If the actor is restarted, the same mailbox will be there. So all
messages on that mailbox will be there as well.

### What happens to the actor

If code within an actor throws an exception, that actor is suspended and the
supervision process is started. Depending on the
supervisor’s decision the actor is resumed (as if nothing happened), restarted
(wiping out its internal state and starting from scratch) or terminated.

@@@note

🎓 For a deeper understanding of the Actor Model, consider the free online course [**Actor Fundamentals**](https://akkademy.akka.io/learn/courses/21/actor-fundamentals) in Akkademy.

@@@
