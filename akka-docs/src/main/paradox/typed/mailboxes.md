# Mailboxes

You are viewing the documentation for the new actor APIs, to view the Akka Classic documentation, see @ref:[Classic Mailboxes](../mailboxes.md).

## Dependency

Mailboxes are part of core Akka, which means that they are part of the `akka-actor` dependency. This
page describes how to use mailboxes with `akka-actor-typed`.

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependency as below.

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor-typed_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction 

Each actor in Akka has a `Mailbox`, this is where the messages are enqueued before being processed by the actor.

By default an unbounded mailbox is used, this means any number of messages can be enqueued into the mailbox. 

The unbounded mailbox is a convenient default but in a scenario where messages are added to the mailbox faster
than the actor can process them, this can lead to the application running out of memory.
For this reason a bounded mailbox can be specified, the bounded mailbox will pass new messages to `deadletters` when the
mailbox is full.

For advanced use cases it is also possible to defer mailbox selection to config by pointing to a config path.

## Selecting what mailbox is used

### Selecting a Mailbox Type for an Actor

To select a specific mailbox for an actor use @apidoc[MailboxSelector](MailboxSelector$) to create a @apidoc[Props](typed.Props) instance for spawning your actor:

Scala
:  @@snip [MailboxDocSpec.scala](/akka-actor-typed-tests/src/test/scala/docs/akka/typed/MailboxDocSpec.scala) { #select-mailbox }

Java
:  @@snip [MailboxDocTest.java](/akka-actor-typed-tests/src/test/java/jdocs/akka/typed/MailboxDocTest.java) { #select-mailbox }

@apidoc[fromConfig](MailboxSelector$) {scala="#fromConfig(path:String):akka.actor.typed.MailboxSelector" java="#fromConfig(java.lang.String)"} takes an absolute config path to a block defining the dispatcher in the config file:

@@snip [MailboxDocSpec.scala](/akka-actor-typed-tests/src/test/resources/mailbox-config-sample.conf) { }

### Default Mailbox

The default mailbox is used when the mailbox is not specified and is the @apidoc[SingleConsumerOnlyUnboundedMailbox]

### Which Configuration is passed to the Mailbox Type

Each mailbox type is implemented by a class which extends @apidoc[MailboxType](MailboxType)
and takes two constructor arguments: a @apidoc[ActorSystem.Settings](typed.Settings) object and
a [Config](https://lightbend.github.io/config/latest/api/index.html?com/typesafe/config/Config.html) section. The latter is computed by obtaining the named
configuration section from the @apidoc[ActorSystem](typed.ActorSystem) configuration, overriding its
`id` key with the configuration path of the mailbox type and adding a
fall-back to the default mailbox configuration section.

## Mailbox Implementations

Akka ships with a number of mailbox implementations:

 * 
   @apidoc[SingleConsumerOnlyUnboundedMailbox](SingleConsumerOnlyUnboundedMailbox) (default)
    * This is the default
    * Backed by a Multiple-Producer Single-Consumer queue, cannot be used with `BalancingDispatcher`
    * Blocking: No
    * Bounded: No
    * Configuration name: `"akka.dispatch.SingleConsumerOnlyUnboundedMailbox"`
 * 
   @apidoc[UnboundedMailbox]
    * Backed by a @javadoc[java.util.concurrent.ConcurrentLinkedQueue](java.util.concurrent.ConcurrentLinkedQueue)
    * Blocking: No
    * Bounded: No
    * Configuration name: `"unbounded"` or `"akka.dispatch.UnboundedMailbox"`

 * 
   @apidoc[NonBlockingBoundedMailbox]
    * Backed by a very efficient Multiple-Producer Single-Consumer queue
    * Blocking: No (discards overflowing messages into deadLetters)
    * Bounded: Yes
    * Configuration name: `"akka.dispatch.NonBlockingBoundedMailbox"`
 * 
   @apidoc[UnboundedControlAwareMailbox]
    * Delivers messages that extend @apidoc[akka.dispatch.ControlMessage](dispatch.ControlMessage) with higher priority
    * Backed by two @javadoc[java.util.concurrent.ConcurrentLinkedQueue](java.util.concurrent.ConcurrentLinkedQueue)
    * Blocking: No
    * Bounded: No
    * Configuration name: `"akka.dispatch.UnboundedControlAwareMailbox"`
 * 
   @apidoc[UnboundedPriorityMailbox]
    * Backed by a @javadoc[java.util.concurrent.PriorityBlockingQueue](java.util.concurrent.PriorityBlockingQueue)
    * Delivery order for messages of equal priority is undefined - contrast with the `UnboundedStablePriorityMailbox`
    * Blocking: No
    * Bounded: No
    * Configuration name: `"akka.dispatch.UnboundedPriorityMailbox"`
 * 
   @apidoc[UnboundedStablePriorityMailbox]
    * Backed by a @javadoc[java.util.concurrent.PriorityBlockingQueue](java.util.concurrent.PriorityBlockingQueue) wrapped in an @apidoc[akka.util.PriorityQueueStabilizer](PriorityQueueStabilizer)
    * FIFO order is preserved for messages of equal priority - contrast with the `UnboundedPriorityMailbox`
    * Blocking: No
    * Bounded: No
    * Configuration name: `"akka.dispatch.UnboundedStablePriorityMailbox"`

Other bounded mailbox implementations which will block the sender if the capacity is reached and
configured with non-zero `mailbox-push-timeout-time`. 

@@@ note

The following mailboxes should only be used with zero `mailbox-push-timeout-time`.

@@@

 * @apidoc[BoundedMailbox]
    * Backed by a @javadoc[java.util.concurrent.LinkedBlockingQueue](java.util.concurrent.LinkedBlockingQueue)
    * Blocking: Yes if used with non-zero `mailbox-push-timeout-time`, otherwise No
    * Bounded: Yes
    * Configuration name: `"bounded"` or `"akka.dispatch.BoundedMailbox"`
 * @apidoc[BoundedPriorityMailbox]
    * Backed by a @javadoc[java.util.PriorityQueue](java.util.PriorityQueue) wrapped in an @apidoc[akka.util.BoundedBlockingQueue](BoundedBlockingQueue)
    * Delivery order for messages of equal priority is undefined - contrast with the `BoundedStablePriorityMailbox`
    * Blocking: Yes if used with non-zero `mailbox-push-timeout-time`, otherwise No
    * Bounded: Yes
    * Configuration name: `"akka.dispatch.BoundedPriorityMailbox"`
 * @apidoc[BoundedStablePriorityMailbox]
    * Backed by a @javadoc[java.util.PriorityQueue](java.util.PriorityQueue) wrapped in an @apidoc[akka.util.PriorityQueueStabilizer](PriorityQueueStabilizer) and an @apidoc[akka.util.BoundedBlockingQueue](BoundedBlockingQueue)
    * FIFO order is preserved for messages of equal priority - contrast with the BoundedPriorityMailbox
    * Blocking: Yes if used with non-zero `mailbox-push-timeout-time`, otherwise No
    * Bounded: Yes
    * Configuration name: `"akka.dispatch.BoundedStablePriorityMailbox"`
 * @apidoc[BoundedControlAwareMailbox]
    * Delivers messages that extend @apidoc[akka.dispatch.ControlMessage](dispatch.ControlMessage) with higher priority
    * Backed by two @javadoc[java.util.concurrent.ConcurrentLinkedQueue](java.util.concurrent.ConcurrentLinkedQueue) and blocking on enqueue if capacity has been reached
    * Blocking: Yes if used with non-zero `mailbox-push-timeout-time`, otherwise No
    * Bounded: Yes
    * Configuration name: `"akka.dispatch.BoundedControlAwareMailbox"`

## Custom Mailbox type

The best way to show how to create your own Mailbox type is by example:

Scala
:   @@snip [MyUnboundedMailbox.scala](/akka-docs/src/test/scala/docs/dispatcher/MyUnboundedMailbox.scala) { #mailbox-marker-interface }

Java
:   @@snip [MyUnboundedMessageQueueSemantics.java](/akka-docs/src/test/java/jdocs/dispatcher/MyUnboundedMessageQueueSemantics.java) { #mailbox-marker-interface }


Scala
:   @@snip [MyUnboundedMailbox.scala](/akka-docs/src/test/scala/docs/dispatcher/MyUnboundedMailbox.scala) { #mailbox-implementation-example }

Java
:   @@snip [MyUnboundedMailbox.java](/akka-docs/src/test/java/jdocs/dispatcher/MyUnboundedMailbox.java) { #mailbox-implementation-example }

And then you specify the FQCN of your MailboxType as the value of the "mailbox-type" in the dispatcher
configuration, or the mailbox configuration.

@@@ note

Make sure to include a constructor which takes
@apidoc[akka.actor.ActorSystem.Settings](actor.ActorSystem.Settings) and [com.typesafe.config.Config](https://lightbend.github.io/config/latest/api/index.html?com/typesafe/config/Config.html)
arguments, as this constructor is invoked reflectively to construct your
mailbox type. The config passed in as second argument is that section from
the configuration which describes the dispatcher or mailbox setting using
this mailbox type; the mailbox type will be instantiated once for each
dispatcher or mailbox setting using it.

@@@

