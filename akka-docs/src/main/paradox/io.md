# I/O

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use I/O, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

The `akka.io` package has been developed in collaboration between the Akka
and [spray.io](http://spray.io) teams. Its design combines experiences from the
`spray-io` module with improvements that were jointly developed for
more general consumption as an actor-based service.

The guiding design goal for this I/O implementation was to reach extreme
scalability, make no compromises in providing an API correctly matching the
underlying transport mechanism and to be fully event-driven, non-blocking and
asynchronous.  The API is meant to be a solid foundation for the implementation
of network protocols and building higher abstractions; it is not meant to be a
full-service high-level NIO wrapper for end users.

## Terminology, Concepts

The I/O API is completely actor based, meaning that all operations are implemented with message passing instead of
direct method calls. Every I/O driver (TCP, UDP) has a special actor, called a *manager* that serves
as an entry point for the API. I/O is broken into several drivers. The manager for a particular driver
is accessible @scala[through the @scaladoc[IO](akka.io.IO$) entry point]@java[by querying an @javadoc[ActorSystem](akka.actor.ActorSystem)]. For example the following code
looks up the TCP manager and returns its @apidoc[ActorRef](akka.actor.ActorRef):

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #manager }

Java
:  @@snip [EchoManager.java](/akka-docs/src/test/java/jdocs/io/japi/EchoManager.java) { #manager }

The manager receives I/O command messages and instantiates worker actors in response. The worker actors present
themselves to the API user in the reply to the command that was sent. For example after a @apidoc[Connect](akka.io.Tcp.Connect) command sent to
the TCP manager the manager creates an actor representing the TCP connection. All operations related to the given TCP
connections can be invoked by sending messages to the connection actor which announces itself by sending a @apidoc[Connected](akka.io.Tcp.Connected)
message.

### DeathWatch and Resource Management

I/O worker actors receive commands and also send out events. They usually need a user-side counterpart actor listening
for these events (such events could be inbound connections, incoming bytes or acknowledgements for writes). These worker
actors *watch* their listener counterparts. If the listener stops then the worker will automatically release any
resources that it holds. This design makes the API more robust against resource leaks.

Thanks to the completely actor based approach of the I/O API the opposite direction works as well: a user actor
responsible for handling a connection can watch the connection actor to be notified if it unexpectedly terminates.

### Write models (Ack, Nack)

I/O devices have a maximum throughput which limits the frequency and size of writes. When an
application tries to push more data than a device can handle, the driver has to buffer bytes until the device
is able to write them. With buffering it is possible to handle short bursts of intensive writes --- but no buffer is infinite.
"Flow control" is needed to avoid overwhelming device buffers.

Akka supports two types of flow control:

 * *Ack-based*, where the driver notifies the writer when writes have succeeded.
 * *Nack-based*, where the driver notifies the writer when writes have failed.

Each of these models is available in both the TCP and the UDP implementations of Akka I/O.

Individual writes can be acknowledged by providing an ack object in the write message (@apidoc[Write](akka.io.Tcp.Write) in the case of TCP and
@apidoc[Send](akka.io.Udp.Send) for UDP). When the write is complete the worker will send the ack object to the writing actor. This can be
used to implement *ack-based* flow control; sending new data only when old data has been acknowledged.

If a write (or any other command) fails, the driver notifies the actor that sent the command with a special message
(`CommandFailed` in the case of UDP and TCP). This message will also notify the writer of a failed write, serving as a
nack for that write. Please note, that in a nack-based flow-control setting the writer has to be prepared for the fact
that the failed write might not be the most recent write it sent. For example, the failure notification for a write
`W1` might arrive after additional write commands `W2` and `W3` have been sent. If the writer wants to resend any
nacked messages it may need to keep a buffer of pending messages.

@@@ warning

An acknowledged write does not mean acknowledged delivery or storage; receiving an ack for a write signals that
the I/O driver has successfully processed the write. The Ack/Nack protocol described here is a means of flow control
not error handling. In other words, data may still be lost, even if every write is acknowledged.

@@@

### ByteString

To maintain isolation, actors should communicate with immutable objects only. @apidoc[ByteString](akka.util.ByteString) is an
immutable container for bytes. It is used by Akka's I/O system as an efficient, immutable alternative
the traditional byte containers used for I/O on the JVM, such as @scala[@scaladoc[Array](scala.Array)[@scaladoc[Byte](scala.Byte)]]@java[`byte[]`] and @javadoc[ByteBuffer](java.nio.ByteBuffer).

`ByteString` is a [rope-like](https://en.wikipedia.org/wiki/Rope_\(computer_science\)) data structure that is immutable
and provides fast concatenation and slicing operations (perfect for I/O). When two `ByteString`s are concatenated
together they are both stored within the resulting `ByteString` instead of copying both to a new @scala[`Array`]@java[array]. Operations
such as @apidoc[drop](akka.util.ByteString) {scala="#drop(n:Int):akka.util.ByteString" java="#drop(int)"} and @apidoc[take](akka.util.ByteString) {scala="#take(n:Int):akka.util.ByteString" java="#take(int)"} return `ByteString`s that still reference the original @scala[`Array`]@java[array], but just change the
offset and length that is visible. Great care has also been taken to make sure that the internal @scala[`Array`]@java[array] cannot be
modified. Whenever a potentially unsafe @scala[`Array`]@java[array] is used to create a new `ByteString` a defensive copy is created. If
you require a `ByteString` that only blocks as much memory as necessary for its content, use the @apidoc[compact](akka.util.ByteString) {scala="#compact:akka.util.CompactByteString" java="#compact()"} method to
get a @apidoc[CompactByteString](akka.util.CompactByteString) instance. If the `ByteString` represented only a slice of the original array, this will
result in copying all bytes in that slice.

`ByteString` inherits all methods from @scaladoc[IndexedSeq](scala.collection.immutable.IndexedSeq), and it also has some new ones. For more information, look up the @apidoc[akka.util.ByteString](akka.util.ByteString) class and @scaladoc[its companion object](akka.util.ByteString$) in the ScalaDoc.

`ByteString` also comes with its own optimized builder and iterator classes @apidoc[ByteStringBuilder](akka.util.ByteStringBuilder) and
@apidoc[ByteIterator](akka.util.ByteIterator) which provide extra features in addition to those of normal builders and iterators.

#### Compatibility with java.io

A @apidoc[ByteStringBuilder](akka.util.ByteStringBuilder) can be wrapped in a @javadoc[java.io.OutputStream](java.io.OutputStream) via the @apidoc[asOutputStream](akka.util.ByteStringBuilder) {scala="#asOutputStream:java.io.OutputStream" java="#asOutputStream()"} method. Likewise, @apidoc[ByteIterator](akka.util.ByteIterator) can be wrapped in a @javadoc[java.io.InputStream](java.io.InputStream) via @apidoc[asInputStream](akka.util.ByteIterator) {scala="#asInputStream:java.io.InputStream" java="#asInputStream()"}. Using these, `akka.io` applications can integrate legacy code based on `java.io` streams.

## Architecture in-depth

For further details on the design and internal architecture see @ref:[I/O Layer Design](common/io-layer.md).

@@toc

@@@ index

* [I/O Layer Design](common/io-layer.md)

@@@
