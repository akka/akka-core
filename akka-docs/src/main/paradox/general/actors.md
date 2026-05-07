---
project.description: What is an Actor and sending messages between independent units of computation in Akka.
---
# What is an Actor?

The previous section about @ref:[Actor Systems](actor-systems.md) explained how actors form
hierarchies and are the smallest unit when building an application. This
section looks at one such actor in isolation, explaining the concepts you
encounter while implementing it. For a more in depth reference with all the
details please refer to @ref:[Introduction to Actors](../typed/actors.md).

The [Actor Model](https://en.wikipedia.org/wiki/Actor_model) as defined by
Hewitt, Bishop and Steiger in 1973 is a computational model that expresses
exactly what it means for computation to be distributed. The processing
units—Actors—can only communicate by exchanging messages and upon reception of a
message an Actor can do the following three fundamental actions:

  1. send a finite number of messages to Actors it knows
  2. create a finite number of new Actors
  3. designate the behavior to be applied to the next message

An actor is a container for @ref:[State](#state), @ref:[Behavior](#behavior), a @ref:[Mailbox](#mailbox), @ref:[Child Actors](#child-actors)
and a @ref:[Supervisor Strategy](#supervisor-strategy). All of this is encapsulated behind an @ref:[Actor Reference](#actor-reference).
One noteworthy aspect is that actors have an explicit lifecycle,
they are not automatically destroyed when no longer referenced; after having
created one, it is your responsibility to make sure that it will eventually be
terminated as well—which also gives you control over how resources are released
@ref:[When an Actor Terminates](#when-an-actor-terminates).

## Actor Reference

As detailed below, an actor object needs to be shielded from the outside in
order to benefit from the actor model. Therefore, actors are represented to the
outside using actor references, which are objects that can be passed around
freely and without restriction. This split into inner and outer object enables
transparency for all the desired operations: restarting an actor without
needing to update references elsewhere, placing the actual actor object on
remote hosts, sending messages to actors independent of where they are running.
But the most important aspect is that it is not possible to look inside an
actor and get hold of its state from the outside, unless the actor unwisely
publishes this information itself.

Actor references are parameterized and only messages that are of the specified
type can be sent to them.

## State

Actor objects will typically contain some variables which reflect possible
states the actor may be in. This can be an explicit state machine,
or it could be a counter, set of listeners, pending requests, etc.
These data are what make an actor valuable, and they
must be protected from corruption by other actors. The good news is that Akka
actors conceptually each have their own light-weight thread, which is
completely shielded from the rest of the system. This means that instead of
having to synchronize access using locks you can write your actor code
without worrying about concurrency at all.

Behind the scenes Akka will run sets of actors on sets of real threads, where
typically many actors share one thread, and subsequent invocations of one actor
may end up being processed on different threads. Akka ensures that this
implementation detail does not affect the single-threadedness of handling the
actor’s state.

Because the internal state is vital to an actor’s operations, having
inconsistent state is fatal. Thus, when the actor fails and is restarted by its
supervisor, the state will be created from scratch, like upon first creating
the actor. This is to enable the ability of self-healing of the system.

Optionally, an actor's state can be automatically recovered to the state
before a restart by persisting received messages and replaying them after
restart (see @ref:[Event Sourcing](../typed/persistence.md)).

## Behavior

Every time a message is processed, it is matched against the current behavior
of the actor. Behavior means a function which defines the actions to be taken
in reaction to the message at that point in time, say forward a request if the
client is authorized, deny it otherwise. This behavior may change over time,
e.g. because different clients obtain authorization over time, or because the
actor may go into an “out-of-service” mode and later come back. These changes
are achieved by either encoding them in state variables which are read from the
behavior logic, or the function itself may be swapped out at runtime, by returning
a different behavior to be used for next message. However, the initial behavior defined
during construction of the actor object is special in the sense that a restart
of the actor will reset its behavior to this initial one.

Messages can be sent to an @ref:[actor Reference](#actor-reference) and behind
this façade there is a behavior that receives the message and acts upon it. The
binding between Actor reference and behavior can change over time, but that is not
visible on the outside.

Actor references are parameterized and only messages that are of the specified type
can be sent to them. The association between an actor reference and its type
parameter must be made when the actor reference (and its Actor) is created.
For this purpose each behavior is also parameterized with the type of messages
it is able to process. Since the behavior can change behind the actor reference
façade, designating the next behavior is a constrained operation: the successor
must handle the same type of messages as its predecessor. This is necessary in
order to not invalidate the actor references that refer to this Actor.

What this enables is that whenever a message is sent to an Actor we can
statically ensure that the type of the message is one that the Actor declares
to handle—we can avoid the mistake of sending completely pointless messages.
What we cannot statically ensure, though, is that the behavior behind the
actor reference will be in a given state when our message is received. The
fundamental reason is that the association between actor reference and behavior
is a dynamic runtime property, the compiler cannot know it while it translates
the source code.

This is the same as for normal Java objects with internal variables: when
compiling the program we cannot know what their value will be, and if the
result of a method call depends on those variables then the outcome is
uncertain to a degree—we can only be certain that the returned value is of a
given type.

The reply message type of an Actor command is described by the type of the
actor reference for the reply-to that is contained within the message. This
allows a conversation to be described in terms of its types: the reply will
be of type A, but it might also contain an address of type B, which then allows
the other Actor to continue the conversation by sending a message of type B to
this new actor reference. While we cannot statically express the “current” state
of an Actor, we can express the current state of a protocol between two Actors,
since that is just given by the last message type that was received or sent.

## Mailbox

An actor’s purpose is the processing of messages, and these messages were sent
to the actor from other actors (or from outside the actor system). The piece
which connects sender and receiver is the actor’s mailbox: each actor has
exactly one mailbox to which all senders enqueue their messages. Enqueuing
happens in the time-order of send operations, which means that messages sent
from different actors may not have a defined order at runtime due to the
apparent randomness of distributing actors across threads. Sending multiple
messages to the same target from the same actor, on the other hand, will
enqueue them in the same order.

There are different mailbox implementations to choose from, the default being a
FIFO: the order of the messages processed by the actor matches the order in
which they were enqueued. This is usually a good default, but applications may
need to prioritize some messages over others. In this case, a priority mailbox
will enqueue not always at the end but at a position as given by the message
priority, which might even be at the front. While using such a queue, the order
of messages processed will naturally be defined by the queue’s algorithm and in
general not be FIFO.

An important feature in which Akka differs from some other actor model
implementations is that the current behavior must always handle the next
dequeued message, there is no scanning the mailbox for the next matching one.
Failure to handle a message will typically be treated as a failure, unless this
behavior is overridden.

## Child Actors

Each actor is potentially a parent: if it creates children for delegating
sub-tasks, it will automatically supervise them. The list of children is
maintained within the actor’s context and the actor has access to it.
Modifications to the list are done by spawning or stopping children and
these actions are reflected immediately. The actual creation and termination
actions happen behind the scenes in an asynchronous way, so they do not “block”
their parent.

## Supervisor Strategy

The final piece of an actor is its strategy for handling unexpected exceptions - failures. 
Fault handling is then done transparently by Akka, applying one of the strategies described 
in @ref:[Fault Tolerance](../typed/fault-tolerance.md) for each failure.

## When an Actor Terminates

Once an actor terminates, i.e. fails in a way which is not handled by a
restart, stops itself or is stopped by its supervisor, it will free up its
resources, draining all remaining messages from its mailbox into the system’s
“dead letter mailbox” which will forward them to the EventStream as DeadLetters.
The mailbox is then replaced within the actor reference with a system mailbox,
redirecting all new messages to the EventStream as DeadLetters. This
is done on a best effort basis, though, so do not rely on it in order to
construct “guaranteed delivery”.

@@@note

🎓 For a deeper understanding of the Actor Model, consider the free online course [**Actor Fundamentals**](https://akkademy.akka.io/learn/courses/21/actor-fundamentals) in Akkademy.

@@@
