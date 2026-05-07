---
project.description: The Akka ActorSystem.
---
# Actor Systems

Actors are objects which encapsulate state and behavior, they communicate
exclusively by exchanging messages which are placed into the recipient’s
mailbox. In a sense, actors are the most stringent form of object-oriented
programming, but it serves better to view them as persons: while modeling a
solution with actors, envision a group of people and assign sub-tasks to them,
arrange their functions into an organizational structure and think about how to
escalate failure (all with the benefit of not actually dealing with people,
which means that we need not concern ourselves with their emotional state or
moral issues). The result can then serve as a mental scaffolding for building
the software implementation.

@@@ note

An ActorSystem is a heavyweight structure that will allocate 1…N Threads,
so create one per logical application.

@@@

## Hierarchical Structure

Like in an economic organization, actors naturally form hierarchies. One actor,
which is to oversee a certain function in the program might want to split up
its task into smaller, more manageable pieces. For this purpose, it starts child
actors.

The quintessential feature of actor systems is that tasks are split up and
delegated until they become small enough to be handled in one piece. In doing
so, not only is the task itself clearly structured, but the resulting actors
can be reasoned about in terms of which messages they should process, how they
should react normally and how failure should be handled.

Compare this to layered software design which easily devolves into defensive
programming with the aim of not leaking any failure out: if the problem is
communicated to the right person, a better solution can be found than if
trying to keep everything “under the carpet”.

Now, the difficulty in designing such a system is how to decide how to
structure the work. There is no single best solution, but there are a few
guidelines which might be helpful:

 * If one actor carries very important data (i.e. its state shall not be lost
   if avoidable), this actor should source out any possibly dangerous sub-tasks
   to children and handle failures of these children as appropriate. Depending on
   the nature of the requests, it may be best to create a new child for each request, 
   which simplifies state management for collecting the replies. This is known as the
   “Error Kernel Pattern” from Erlang.
 * If one actor depends on another actor for carrying out its duty, it should
   watch that other actor’s liveness and act upon receiving a termination
   notice.
 * If one actor has multiple responsibilities each responsibility can often be pushed
   into a separate child to make the logic and state more simple.

## Configuration Container

The actor system as a collaborating ensemble of actors is the natural unit for
managing shared facilities like scheduling services, configuration, logging,
etc. Several actor systems with different configurations may co-exist within the
same JVM without problems, there is no global shared state within Akka itself,
however the most common scenario will only involve a single actor system per JVM.

Couple this with the transparent communication between actor systems — within one
node or across a network connection — and actor systems are a perfect fit to form
a distributed application.

## Actor Best Practices

 1. Actors should be like nice co-workers: do their job efficiently without
    bothering everyone else needlessly and avoid hogging resources. Translated
    to programming this means to process events and generate responses (or more
    requests) in an event-driven manner. Actors should not block (i.e. passively
    wait while occupying a Thread) on some external entity—which might be a
    lock, a network socket, etc.—unless it is unavoidable; in the latter case
    see @ref:[`Blocking Needs Careful Management`](../typed/dispatchers.md#blocking-management).
 2. Do not pass mutable objects between actors. In order to ensure that, prefer
    immutable messages. If the encapsulation of actors is broken by exposing
    their mutable state to the outside, you are back in normal Java concurrency
    land with all the drawbacks.
 3. Actors are made to be containers for behavior and state, embracing this
    means to not routinely send behavior within messages (which may be tempting
    using Scala closures). One of the risks is to accidentally share mutable
    state between actors, and this violation of the actor model unfortunately
    breaks all the properties which make programming in actors such a nice
    experience.
 4. The top-level actor of the actor system is the innermost part of your 
    Error Kernel, it should only be responsible for starting the various 
    sub systems of your application, and not contain much logic in itself, 
    prefer truly hierarchical systems. This has benefits with
    respect to fault-handling (both considering the granularity of configuration
    and the performance) and it also reduces the strain on the guardian actor,
    which is a single point of contention if over-used.

## What you should not concern yourself with

An actor system manages the resources it is configured to use in order to run
the actors which it contains. There may be millions of actors within one such
system, after all the mantra is to view them as abundant and they weigh in at
an overhead of only roughly 300 bytes per instance. Naturally, the exact order
in which messages are processed in large systems is not controllable by the
application author, but this is also not intended. Take a step back and relax
while Akka does the heavy lifting under the hood.

## Terminating ActorSystem

When you know everything is done for your application, you can have the user guardian
 actor stop, or call the @scala[@scaladoc[terminate](akka.actor.typed.ActorSystem#terminate():Unit)]@java[@javadoc[terminate()](akka.actor.typed.ActorSystem#terminate())] method of @apidoc[ActorSystem](typed.ActorSystem). That will run @ref:[`CoordinatedShutdown`](../coordinated-shutdown.md)
stopping all running actors.

If you want to execute some operations while terminating @apidoc[ActorSystem](typed.ActorSystem),
look at @ref:[`CoordinatedShutdown`](../coordinated-shutdown.md).

@@@note

🎓 For a deeper understanding of the Actor Model, consider the free online course [**Actor Fundamentals**](https://akkademy.akka.io/learn/courses/21/actor-fundamentals) in Akkademy.

@@@
