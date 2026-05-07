---
project.description: Low level API for using TCP with classic actors.
---
# Using TCP

## Dependency

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use TCP, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group="com.typesafe.akka"
  artifact="akka-actor_$scala.binary.version$"
  version=AkkaVersion
}

## Introduction

The code snippets through-out this section assume the following imports:

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #imports }

Java
:  @@snip [IODocTest.java](/akka-docs/src/test/java/jdocs/io/japi/IODocTest.java) { #imports }

All of the Akka I/O APIs are accessed through manager objects. When using an I/O API, the first step is to acquire a
reference to the appropriate manager. The code below shows how to acquire a reference to the @apidoc[akka.io.Tcp$] manager.

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #manager }

Java
:  @@snip [EchoManager.java](/akka-docs/src/test/java/jdocs/io/japi/EchoManager.java) { #manager }

The manager is an actor that handles the underlying low level I/O resources (selectors, channels) and instantiates
workers for specific tasks, such as listening to incoming connections.

## Connecting

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #client }

Java
:  @@snip [IODocTest.java](/akka-docs/src/test/java/jdocs/io/japi/IODocTest.java) { #client }

The first step of connecting to a remote address is sending a 
@scala[@scaladoc[Connect](akka.io.Tcp.Connect) message]@java[message by the @javadoc[TcpMessage.connect](akka.io.TcpMessage#connect(java.net.InetSocketAddress)) method] to the TCP manager; in addition to the simplest form shown above there
is also the possibility to specify a local @javadoc[InetSocketAddress](java.net.InetSocketAddress) to bind
to and a list of socket options to apply.

@@@ note

The SO_NODELAY (TCP_NODELAY on Windows) socket option defaults to true in
Akka, independently of the OS default settings. This setting disables Nagle's
algorithm, considerably improving latency for most applications. This setting
could be overridden by passing `SO.TcpNoDelay(false)` in the list of socket
options of the @scala[`Connect` message]@java[message by the `TcpMessage.connect` method].

@@@

The TCP manager will then reply either with a @apidoc[akka.io.Tcp.CommandFailed] or it will
spawn an internal actor representing the new connection. This new actor will
then send a @apidoc[akka.io.Tcp.Connected] message to the original sender of the
@scala[@scaladoc[Connect](akka.io.Tcp.Connect) message]@java[message by the @javadoc[TcpMessage.connect](akka.io.TcpMessage#connect(java.net.InetSocketAddress)) method].

In order to activate the new connection a @scala[@scaladoc[Register](akka.io.Tcp.Register) message]@java[message by the @javadoc[TcpMessage.register](akka.io.TcpMessage#register(akka.actor.ActorRef)) method] must be
sent to the connection actor, informing that one about who shall receive data
from the socket. Before this step is done the connection cannot be used, and
there is an internal timeout after which the connection actor will shut itself
down if no @scala[`Register` message]@java[message by the `TcpMessage.register` method] message is received.

The connection actor watches the registered handler and closes the connection
when that one terminates, thereby cleaning up all internal resources associated
with that connection.

The actor in the example above uses `become` to switch from unconnected
to connected operation, demonstrating the commands and events which are
observed in that state. For a discussion on `CommandFailed` see
[Throttling Reads and Writes](#throttling-reads-and-writes) below. @apidoc[akka.io.Tcp.ConnectionClosed] is a trait,
which marks the different connection close events. The last line handles all
connection close events in the same way. It is possible to listen for more
fine-grained connection close events, see @ref:[Closing Connections](#closing-connections) below.

## Accepting connections

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #server }

Java
:  @@snip [IODocTest.java](/akka-docs/src/test/java/jdocs/io/japi/IODocTest.java) { #server }

To create a TCP server and listen for inbound connections, a @scala[@scaladoc[Bind](akka.io.Tcp.Bind) command]@java[message by the @javadoc[TcpMessage.bind](akka.io.TcpMessage#bind(akka.actor.ActorRef,java.net.InetSocketAddress,int)) method]
has to be sent to the TCP manager.  This will instruct the TCP manager
to listen for TCP connections on a particular @javadoc[InetSocketAddress](java.net.InetSocketAddress); the
port may be specified as `0` in order to bind to a random port.

The actor sending the @scala[`Bind` message]@java[message by the `TcpMessage.bind` method] will receive a @apidoc[akka.io.Tcp.Bound]
message signaling that the server is ready to accept incoming connections;
this message also contains the `InetSocketAddress` to which the socket
was actually bound (i.e. resolved IP address and correct port number). 

From this point forward the process of handling connections is the same as for
outgoing connections. The example demonstrates that handling the reads from a
certain connection can be delegated to another actor by naming it as the
handler when sending the @scala[@scaladoc[Register](akka.io.Tcp.Register) message]@java[message by the @javadoc[TcpMessage.register](akka.io.TcpMessage#register(akka.actor.ActorRef)) method]. Writes can be sent from any
actor in the system to the connection actor (i.e. the actor which sent the
`Connected` message). The simplistic handler is defined as:

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/io/IODocSpec.scala) { #simplistic-handler }

Java
:  @@snip [IODocTest.java](/akka-docs/src/test/java/jdocs/io/japi/IODocTest.java) { #simplistic-handler }

For a more complete sample which also takes into account the possibility of
failures when sending please see @ref:[Throttling Reads and Writes](#throttling-reads-and-writes) below.

The only difference to outgoing connections is that the internal actor managing
the listen port—the sender of the @apidoc[akka.io.Tcp.Bound] message—watches the actor
which was named as the recipient for @apidoc[akka.io.Tcp.Connected] messages in the
@scala[`Bind` message]@java[`TcpMessage.bind` method]. When that actor terminates the listen port will be
closed and all resources associated with it will be released; existing
connections will not be terminated at this point.

## Closing connections

A connection can be closed by sending @scala[one of the commands `Close`, `ConfirmedClose` or `Abort`]
@java[a message by one of the methods `TcpMessage.close`, `TcpMessage.confirmedClose` or `TcpMessage.abort`]
to the connection actor.

@scala[@scaladoc[Close](akka.io.Tcp.Close$)]@java[@javadoc[TcpMessage.close](akka.io.TcpMessage#close())] will close the connection by sending a `FIN` message, but without waiting for confirmation from
the remote endpoint. Pending writes will be flushed. If the close is successful, the listener will be notified with
@scaladoc[Closed](akka.io.Tcp.Closed$).

@scala[@scaladoc[ConfirmedClose](akka.io.Tcp.ConfirmedClose$)]@java[@javadoc[TcpMessage.confirmedClose](akka.io.TcpMessage#confirmedClose())] will close the sending direction of the connection by sending a `FIN` message, but data 
will continue to be received until the remote endpoint closes the connection, too. Pending writes will be flushed. If the close is
successful, the listener will be notified with @scaladoc[ConfirmedClosed](akka.io.Tcp.ConfirmedClosed$).

@scala[@scaladoc[Abort](akka.io.Tcp.Abort$)]@java[@javadoc[TcpMessage.abort](akka.io.TcpMessage#abort())] will immediately terminate the connection by sending a `RST` message to the remote endpoint. Pending
writes will be not flushed. If the close is successful, the listener will be notified with @scaladoc[Aborted](akka.io.Tcp.Aborted$).

@scaladoc[PeerClosed](akka.io.Tcp.PeerClosed$) will be sent to the listener if the connection has been closed by the remote endpoint. Per default, the
connection will then automatically be closed from this endpoint as well. To support half-closed connections set the
`keepOpenOnPeerClosed` member of the @scala[@scaladoc[Register](akka.io.Tcp.Register) message]@java[@javadoc[TcpMessage.register](akka.io.TcpMessage#register(akka.actor.ActorRef,boolean,boolean)) method] to `true` in which case the connection stays open until
it receives one of the above close commands.

@apidoc[akka.io.Tcp.ErrorClosed] will be sent to the listener whenever an error happened that forced the connection to be closed.

All close notifications are sub-types of @apidoc[akka.io.Tcp.ConnectionClosed] so listeners who do not need fine-grained close events
may handle all close events in the same way.

## Writing to a connection

Once a connection has been established data can be sent to it from any actor in the form of a @apidoc[Tcp.WriteCommand](akka.io.Tcp.WriteCommand).
`Tcp.WriteCommand` is an abstract class with three concrete implementations:

Tcp.Write
: The simplest `WriteCommand` implementation which wraps a `ByteString` instance and an "ack" event.
A `ByteString` (as explained in @ref:[this section](io.md#bytestring)) models one or more chunks of immutable
in-memory data with a maximum (total) size of 2 GB (2^31 bytes).

Tcp.WriteFile
: If you want to send "raw" data from a file you can do so efficiently with the @apidoc[Tcp.WriteFile](akka.io.Tcp.WriteFile) command.
This allows you do designate a (contiguous) chunk of on-disk bytes for sending across the connection without
the need to first load them into the JVM memory. As such `Tcp.WriteFile` can "hold" more than 2GB of data and
an "ack" event if required.

Tcp.CompoundWrite
: 
Sometimes you might want to group (or interleave) several @apidoc[Tcp.Write](akka.io.Tcp.Write) and/or @apidoc[Tcp.WriteFile](akka.io.Tcp.WriteFile) commands into
one atomic write command which gets written to the connection in one go. The @apidoc[Tcp.CompoundWrite](akka.io.Tcp.CompoundWrite) allows you
to do just that and offers three benefits:
 1. As explained in the following section the TCP connection actor can only handle one single write command at a time.
By combining several writes into one `CompoundWrite` you can have them be sent across the connection with
minimum overhead and without the need to spoon feed them to the connection actor via an *ACK-based* message
protocol.
 2. Because a @apidoc[akka.io.Tcp.WriteCommand] is atomic you can be sure that no other actor can "inject" other writes into your
series of writes if you combine them into one single `CompoundWrite`. In scenarios where several actors write
to the same connection this can be an important feature which can be somewhat hard to achieve otherwise.
 3. The "sub writes" of a `CompoundWrite` are regular @scala[@scaladoc[Write](akka.io.Tcp$$Write) or @scaladoc[WriteFile](akka.io.Tcp$$WriteFile) commands]@java[messages by @javadoc[TcpMessage.write](akka.io.TcpMessage#write(akka.util.ByteString,akka.io.Tcp.Event)) or @javadoc[TcpMessage.writeFile](akka.io.TcpMessage#writeFile(java.lang.String,long,long,akka.io.Tcp.Event)) methods] that themselves can request
"ack" events. These ACKs are sent out as soon as the respective "sub write" has been completed. This allows you to
attach more than one ACK to a @scala[`Write` or `WriteFile`]@java[message by `TcpMessage.write` or `TcpMessage.writeFile`] (by combining it with an empty write that itself requests
an ACK) or to have the connection actor acknowledge the progress of transmitting the `CompoundWrite` by sending
out intermediate ACKs at arbitrary points.


## Throttling Reads and Writes

The basic model of the TCP connection actor is that it has no internal
buffering (i.e. it can only process one write at a time, meaning it can buffer
one write until it has been passed on to the O/S kernel in full). Congestion
needs to be handled at the user level, for both writes and reads.

For back-pressuring writes there are three modes of operation

 * *ACK-based:* every @apidoc[akka.io.Tcp.Write] command carries an arbitrary object, and if
this object is not @apidoc[akka.io.Tcp.NoAck] then it will be returned to the sender of
the `Write` upon successfully writing all contained data to the
socket. If no other write is initiated before having received this
acknowledgement then no failures can happen due to buffer overrun.
 * *NACK-based:* every write which arrives while a previous write is not yet
completed will be replied to with a @apidoc[akka.io.Tcp.CommandFailed] message containing
the failed write. Just relying on this mechanism requires the implemented
protocol to tolerate skipping writes (e.g. if each write is a valid message
on its own and it is not required that all are delivered). This mode is
enabled by setting the `useResumeWriting` flag to `false` within the
   @scala[@scaladoc[Register](akka.io.Tcp.Register) message]@java[@javadoc[TcpMessage.register](akka.io.TcpMessage#register(akka.actor.ActorRef,boolean,boolean)) method] during connection activation.
 * *NACK-based with write suspending:* this mode is very similar to the
NACK-based one, but once a single write has failed no further writes will
succeed until a @scala[@scaladoc[ResumeWriting](akka.io.Tcp.ResumeWriting$) message]@java[message by the @javadoc[TcpMessage.resumeWriting](akka.io.TcpMessage#resumeWriting()) method] is received. This message will
be answered with a @apidoc[akka.io.Tcp.WritingResumed] message once the last accepted
write has completed. If the actor driving the connection implements buffering
and resends the NACK’ed messages after having awaited the
`WritingResumed` signal then every message is delivered exactly once
to the network socket.

These write back-pressure models (with the exception of the second which is rather specialised) are
demonstrated in complete examples below. The full and contiguous source is
available @scala[@extref[on GitHub](github:akka-docs/src/test/scala/docs/io/EchoServer.scala)]@java[@extref[on GitHub](github:akka-docs/rst/java/code/jdocs/io/japi)].

For back-pressuring reads there are two modes of operation

 * *Push-reading:* in this mode the connection actor sends the registered reader actor
incoming data as soon as available as @apidoc[akka.io.Tcp.Received] events. Whenever the reader actor
wants to signal back-pressure to the remote TCP endpoint it can send a @scala[@scaladoc[SuspendReading](akka.io.Tcp.SuspendReading$) message]@java[message by the @javadoc[TcpMessage.suspendReading](akka.io.TcpMessage#suspendReading()) method] 
to the connection actor to indicate that it wants to suspend the
reception of new data. No `Received` events will arrive until a corresponding
@scaladoc[ResumeReading](akka.io.Tcp.ResumeReading$) is sent indicating that the receiver actor is ready again.
 * *Pull-reading:* after sending a `Received` event the connection
actor automatically suspends accepting data from the socket until the reader actor signals
with a `ResumeReading` message that it is ready to process more input data. Hence
new data is "pulled" from the connection by sending `ResumeReading` messages.

@@@ note

It should be obvious that all these flow control schemes only work between
one writer/reader and one connection actor; as soon as multiple actors send write
commands to a single connection no consistent result can be achieved.

@@@

## ACK-Based Write Back-Pressure

For proper function of the following example it is important to configure the
connection to remain half-open when the remote side closed its writing end:
this allows the example `EchoHandler` to write all outstanding data back
to the client before fully closing the connection. This is enabled using a flag
upon connection activation (observe the @scala[@scaladoc[Register](akka.io.Tcp.Register) message]@java[@javadoc[TcpMessage.register](akka.io.TcpMessage#register(akka.actor.ActorRef,boolean,boolean)) method]):

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #echo-manager }

Java
:  @@snip [EchoManager.java](/akka-docs/src/test/java/jdocs/io/japi/EchoManager.java) { #echo-manager }

With this preparation let us dive into the handler itself:

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #simple-echo-handler }

Java
:  @@snip [SimpleEchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/SimpleEchoHandler.java) { #simple-echo-handler }

The principle is simple: when having written a chunk always wait for the
`Ack` to come back before sending the next chunk. While waiting we switch
behavior such that new incoming data are buffered. The helper functions used
are a bit lengthy but not complicated:

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #simple-helpers }

Java
:  @@snip [SimpleEchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/SimpleEchoHandler.java) { #simple-helpers }

The most interesting part is probably the last: an `Ack` removes the oldest
data chunk from the buffer, and if that was the last chunk then we either close
the connection (if the peer closed its half already) or return to the idle
behavior; otherwise we send the next buffered chunk and stay waiting for
the next `Ack`.

Back-pressure can be propagated also across the reading side back to the writer
on the other end of the connection by sending the @scala[@scaladoc[SuspendReading](akka.io.Tcp.SuspendReading$) command]@java[message by the @javadoc[TcpMessage.suspendReading](akka.io.TcpMessage#suspendReading()) method]
to the connection actor. This will lead to no data being read from the
socket anymore (although this does happen after a delay because it takes some
time until the connection actor processes this command, hence appropriate
head-room in the buffer should be present), which in turn will lead to the O/S
kernel buffer filling up on our end, then the TCP window mechanism will stop
the remote side from writing, filling up its write buffer, until finally the
writer on the other side cannot push any data into the socket anymore. This is
how end-to-end back-pressure is realized across a TCP connection.

## NACK-Based Write Back-Pressure with Suspending

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #echo-handler }

Java
:  @@snip [EchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/EchoHandler.java) { #echo-handler }

The principle here is to keep writing until a @apidoc[akka.io.Tcp.CommandFailed] is
received, using acknowledgements only to prune the resend buffer. When a such a
failure was received, transition into a different state for handling and handle
resending of all queued data:

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #buffering }

Java
:  @@snip [EchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/EchoHandler.java) { #buffering }

It should be noted that all writes which are currently buffered have also been
sent to the connection actor upon entering this state, which means that the
@scala[@scaladoc[ResumeWriting](akka.io.Tcp.ResumeWriting$) message]@java[message by the @javadoc[TcpMessage.resumeWriting](akka.io.TcpMessage#resumeWriting()) method] is enqueued after those writes, leading to the
reception of all outstanding `CommandFailed` messages (which are ignored
in this state) before receiving the `WritingResumed` signal. That latter
message is sent by the connection actor only once the internally queued write
has been fully completed, meaning that a subsequent write will not fail. This
is exploited by the `EchoHandler` to switch to an ACK-based approach for
the first ten writes after a failure before resuming the optimistic
write-through behavior.

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #closing }

Java
:  @@snip [EchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/EchoHandler.java) { #closing }

Closing the connection while still sending all data is a bit more involved than
in the ACK-based approach: the idea is to always send all outstanding messages
and acknowledge all successful writes, and if a failure happens then switch
behavior to await the `WritingResumed` event and start over.

The helper functions are very similar to the ACK-based case:

Scala
:  @@snip [EchoServer.scala](/akka-docs/src/test/scala/docs/io/EchoServer.scala) { #helpers }

Java
:  @@snip [EchoHandler.java](/akka-docs/src/test/java/jdocs/io/japi/EchoHandler.java) { #helpers }

## Read Back-Pressure with Pull Mode

When using push based reading, data coming from the socket is sent to the actor as soon
as it is available. In the case of the previous Echo server example
this meant that we needed to maintain a buffer of incoming data to keep it around
since the rate of writing might be slower than the rate of the arrival of new data.

With the Pull mode this buffer can be completely eliminated as the following snippet
demonstrates:

Scala
:  @@snip [ReadBackPressure.scala](/akka-docs/src/test/scala/docs/io/ReadBackPressure.scala) { #pull-reading-echo }

Java
:  @@snip [JavaReadBackPressure.java](/akka-docs/src/test/java/jdocs/io/JavaReadBackPressure.java) { #pull-reading-echo }

The idea here is that reading is not resumed until the previous write has been
completely acknowledged by the connection actor. Every pull mode connection
actor starts from suspended state. To start the flow of data we send a
@scala[@scaladoc[ResumeReading](akka.io.Tcp.ResumeReading$)]@java[message by the @javadoc[TcpMessage.resumeReading](akka.io.TcpMessage#resumeReading()) method] in the `preStart` method to tell the connection actor that
we are ready to receive the first chunk of data. Since we only resume reading when
the previous data chunk has been completely written there is no need for maintaining
a buffer.

To enable pull reading on an outbound connection the `pullMode` parameter of
the @scala[@scaladoc[Connect](akka.io.Tcp.Connect)]@java[@javadoc[TcpMessage.connect](akka.io.TcpMessage#connect(java.net.InetSocketAddress,java.net.InetSocketAddress,java.lang.Iterable,scala.concurrent.duration.FiniteDuration,boolean)) method] should be set to `true`:

Scala
:  @@snip [ReadBackPressure.scala](/akka-docs/src/test/scala/docs/io/ReadBackPressure.scala) { #pull-mode-connect }

Java
:  @@snip [JavaReadBackPressure.java](/akka-docs/src/test/java/jdocs/io/JavaReadBackPressure.java) { #pull-mode-connect }

### Pull Mode Reading for Inbound Connections

The previous section demonstrated how to enable pull reading mode for outbound
connections but it is possible to create a listener actor with this mode of reading
by setting the `pullMode` parameter of the @scala[@scaladoc[Bind](akka.io.Tcp.Bind) command]@java[message by the @javadoc[TcpMessage.bind](akka.io.TcpMessage#bind(akka.actor.ActorRef,java.net.InetSocketAddress,int,java.lang.Iterable,boolean)) method] to `true`:

Scala
:  @@snip [ReadBackPressure.scala](/akka-docs/src/test/scala/docs/io/ReadBackPressure.scala) { #pull-mode-bind }

Java
:  @@snip [JavaReadBackPressure.java](/akka-docs/src/test/java/jdocs/io/JavaReadBackPressure.java) { #pull-mode-bind }

One of the effects of this setting is that all connections accepted by this listener
actor will use pull mode reading.

Another effect of this setting is that in addition of setting all inbound connections to
pull mode, accepting connections becomes pull based, too. This means that after handling
one (or more) `Connected` events the listener actor has to be resumed by sending
it a @scala[`ResumeAccepting` message]@java[message by the `TcpMessage.resumeAccepting` method].

Listener actors with pull mode start suspended so to start accepting connections
a @scala[@scaladoc[ResumeAccepting](akka.io.Tcp.ResumeAccepting) command]@java[message by the @javadoc[TcpMessage.resumeAccepting](akka.io.TcpMessage#resumeAccepting(int)) method] has to be sent to the listener actor after binding was successful:

Scala
:  @@snip [ReadBackPressure.scala](/akka-docs/src/test/scala/docs/io/ReadBackPressure.scala) { #pull-accepting #pull-accepting-cont }

Java
:  @@snip [JavaReadBackPressure.java](/akka-docs/src/test/java/jdocs/io/JavaReadBackPressure.java) { #pull-accepting }

As shown in the example, after handling an incoming connection we need to resume accepting again.

The @scala[`ResumeAccepting`]@java[`TcpMessage.resumeAccepting` method] accepts a `batchSize` parameter that specifies how
many new connections are accepted before a next `ResumeAccepting` message
is needed to resume handling of new connections.
