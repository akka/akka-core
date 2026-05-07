# Frequently Asked Questions

## Akka Project

### Where does the name Akka come from?

It is the name of a beautiful Swedish [mountain](https://en.wikipedia.org/wiki/%C3%81hkk%C3%A1)
up in the northern part of Sweden called Laponia. The mountain is also sometimes
called 'The Queen of Laponia'.

Akka is also the name of a goddess in the Sámi (the native Swedish population)
mythology. She is the goddess that stands for all the beauty and good in the
world. The mountain can be seen as the symbol of this goddess.

Also, the name AKKA is a palindrome of the letters A and K as in Actor Kernel.

Akka is also:

 * the name of the goose that Nils traveled across Sweden on in [The Wonderful Adventures of Nils](https://en.wikipedia.org/wiki/The_Wonderful_Adventures_of_Nils) by the Swedish writer Selma Lagerlöf.
 * the Finnish word for 'nasty elderly woman' and the word for 'elder sister' in the Indian languages Tamil, Telugu, Kannada and Marathi.
 * a [font](https://www.dafont.com/akka.font)
 * a town in Morocco
 * a near-earth asteroid

## Resources with Explicit Lifecycle

Actors, ActorSystems, Materializers (for streams), all these types of objects bind
resources that must be released explicitly. The reason is that Actors are meant to have
a life of their own, existing independently of whether messages are currently en route
to them. Therefore you should always make sure that for every creation of such an object
you have a matching `stop`, `terminate`, or `shutdown` call implemented.

In particular you typically want to bind such values to immutable references, i.e.
`final ActorSystem system` in Java or `val system: ActorSystem` in Scala.

### JVM application or Scala REPL “hanging”

Due to an ActorSystem’s explicit lifecycle the JVM will not exit until it is stopped.
Therefore it is necessary to shutdown all ActorSystems within a running application or
Scala REPL session in order to allow these processes to terminate.

Shutting down an ActorSystem will properly terminate all Actors and Materializers
that were created within it.

## Actors

### Why OutOfMemoryError?

It can be many reasons for OutOfMemoryError. For example, in a pure push based system with
message consumers that are potentially slower than corresponding message producers you must
add some kind of message flow control. Otherwise messages will be queued in the consumers'
mailboxes and thereby filling up the heap memory.

## Cluster

### How reliable is the message delivery?

The general rule is **at-most-once delivery**, i.e. no guaranteed delivery.
Stronger reliability can be built on top, and Akka provides tools to do so.

Read more in @ref:[Message Delivery Reliability](../general/message-delivery-reliability.md).

## Debugging

### How do I turn on debug logging?

To turn on debug logging in your actor system add the following to your configuration:

```
akka.loglevel = DEBUG
```

Read more about it in the docs for @ref:[Logging](../typed/logging.md).

# Other questions?

Do you have a question not covered here? Find out how to
[get involved in the community](https://discuss.akka.io) or
[set up a time](https://akka.io/contact-us) to discuss enterprise-grade
expert support from [Akka](akka.io/).
