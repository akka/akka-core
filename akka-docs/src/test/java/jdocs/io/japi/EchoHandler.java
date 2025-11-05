/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io.japi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.ConnectionClosed;
import akka.io.Tcp.Event;
import akka.io.Tcp.Received;
import akka.io.Tcp.Write;
import akka.io.Tcp.WritingResumed;
import akka.io.TcpMessage;
import akka.util.ByteString;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;

// #echo-handler
public class EchoHandler extends AbstractActor {

  final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), getSelf());

  final ActorRef connection;
  final InetSocketAddress remote;

  public static final long MAX_STORED = 100000000;
  public static final long HIGH_WATERMARK = MAX_STORED * 5 / 10;
  public static final long LOW_WATERMARK = MAX_STORED * 2 / 10;

  private long transferred;
  private int storageOffset = 0;
  private long stored = 0;
  private Queue<ByteString> storage = new LinkedList<>();

  private boolean suspended = false;

  private static class Ack implements Event {
    public final int ack;

    public Ack(int ack) {
      this.ack = ack;
    }
  }

  public EchoHandler(ActorRef connection, InetSocketAddress remote) {
    this.connection = connection;
    this.remote = remote;

    writing = writing();

    // sign death pact: this actor stops when the connection is closed
    getContext().watch(connection);

    // start out in optimistic write-through mode
    getContext().become(writing);
  }

  @Override
  public Receive createReceive() {
    return writing;
  }

  private final Receive writing;

  private Receive writing() {
    return receiveBuilder()
        .match(
            Received.class,
            msg -> {
              final ByteString data = msg.data();
              connection.tell(TcpMessage.write(data, new Ack(currentOffset())), getSelf());
              buffer(data);
            })
        .match(
            Integer.class,
            msg -> {
              acknowledge(msg);
            })
        .match(
            CommandFailed.class,
            msg -> {
              final Write w = (Write) msg.cmd();
              connection.tell(TcpMessage.resumeWriting(), getSelf());
              getContext().become(buffering((Ack) w.ack()));
            })
        .match(
            ConnectionClosed.class,
            msg -> {
              if (msg.isPeerClosed()) {
                if (storage.isEmpty()) {
                  getContext().stop(getSelf());
                } else {
                  getContext().become(closing());
                }
              }
            })
        .build();
  }

  // #buffering

  static final class BufferingState {
    int toAck = 10;
    boolean peerClosed = false;
  }

  protected Receive buffering(final Ack nack) {
    final BufferingState state = new BufferingState();

    return receiveBuilder()
        .match(
            Received.class,
            msg -> {
              buffer(msg.data());
            })
        .match(
            WritingResumed.class,
            msg -> {
              writeFirst();
            })
        .match(
            ConnectionClosed.class,
            msg -> {
              if (msg.isPeerClosed()) state.peerClosed = true;
              else getContext().stop(getSelf());
            })
        .match(
            Integer.class,
            ack -> {
              acknowledge(ack);

              if (ack >= nack.ack) {
                // otherwise it was the ack of the last successful write

                if (storage.isEmpty()) {
                  if (state.peerClosed) getContext().stop(getSelf());
                  else getContext().become(writing);

                } else {
                  if (state.toAck > 0) {
                    // stay in ACK-based mode for a short while
                    writeFirst();
                    --state.toAck;
                  } else {
                    // then return to NACK-based again
                    writeAll();
                    if (state.peerClosed) getContext().become(closing());
                    else getContext().become(writing);
                  }
                }
              }
            })
        .build();
  }
  // #buffering

  // #closing
  protected Receive closing() {
    return receiveBuilder()
        .match(
            CommandFailed.class,
            msg -> {
              // the command can only have been a Write
              connection.tell(TcpMessage.resumeWriting(), getSelf());
              getContext().become(closeResend(), false);
            })
        .match(
            Integer.class,
            msg -> {
              acknowledge(msg);
              if (storage.isEmpty()) getContext().stop(getSelf());
            })
        .build();
  }

  protected Receive closeResend() {
    return receiveBuilder()
        .match(
            WritingResumed.class,
            msg -> {
              writeAll();
              getContext().unbecome();
            })
        .match(
            Integer.class,
            msg -> {
              acknowledge(msg);
            })
        .build();
  }
  // #closing

  // #storage-omitted

  @Override
  public void postStop() {
    log.info("transferred {} bytes from/to [{}]", transferred, remote);
  }

  // #helpers
  protected void buffer(ByteString data) {
    storage.add(data);
    stored += data.size();

    if (stored > MAX_STORED) {
      log.warning("drop connection to [{}] (buffer overrun)", remote);
      getContext().stop(getSelf());

    } else if (stored > HIGH_WATERMARK) {
      log.debug("suspending reading at {}", currentOffset());
      connection.tell(TcpMessage.suspendReading(), getSelf());
      suspended = true;
    }
  }

  protected void acknowledge(int ack) {
    assertEquals(storageOffset, ack);
    assertFalse(storage.isEmpty());

    final ByteString acked = storage.remove();
    stored -= acked.size();
    transferred += acked.size();
    storageOffset += 1;

    if (suspended && stored < LOW_WATERMARK) {
      log.debug("resuming reading");
      connection.tell(TcpMessage.resumeReading(), getSelf());
      suspended = false;
    }
  }
  // #helpers

  protected int currentOffset() {
    return storageOffset + storage.size();
  }

  protected void writeAll() {
    int i = 0;
    for (ByteString data : storage) {
      connection.tell(TcpMessage.write(data, new Ack(storageOffset + i++)), getSelf());
    }
  }

  protected void writeFirst() {
    connection.tell(TcpMessage.write(storage.peek(), new Ack(storageOffset)), getSelf());
  }

  // #storage-omitted
}
// #echo-handler
