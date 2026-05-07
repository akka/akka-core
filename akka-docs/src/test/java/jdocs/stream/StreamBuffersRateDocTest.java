/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.Arrays;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamBuffersRateDocTest extends AbstractJavaTest {

  static class Job {}

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("StreamBuffersDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  final SilenceSystemOut.System System = SilenceSystemOut.get();

  @Test
  public void demonstratePipelining() {
    // #pipelining
    Source.from(Arrays.asList(1, 2, 3))
        .map(
            i -> {
              System.out.println("A: " + i);
              return i;
            })
        .async()
        .map(
            i -> {
              System.out.println("B: " + i);
              return i;
            })
        .async()
        .map(
            i -> {
              System.out.println("C: " + i);
              return i;
            })
        .async()
        .runWith(Sink.ignore(), system);
    // #pipelining
  }

  @Test
  @SuppressWarnings("unused")
  public void demonstrateBufferSizes() {
    // #section-buffer
    final Flow<Integer, Integer, NotUsed> flow1 =
        Flow.of(Integer.class)
            .map(elem -> elem * 2)
            .async()
            .addAttributes(Attributes.inputBuffer(1, 1)); // the buffer size of this map is 1
    final Flow<Integer, Integer, NotUsed> flow2 =
        flow1
            .via(Flow.of(Integer.class).map(elem -> elem / 2))
            .async(); // the buffer size of this map is the value from the surrounding graph it is
    // used in
    final RunnableGraph<NotUsed> runnableGraph =
        Source.range(1, 10).via(flow1).to(Sink.foreach(elem -> System.out.println(elem)));

    final RunnableGraph<NotUsed> withOverridenDefaults =
        runnableGraph.withAttributes(Attributes.inputBuffer(64, 64));
    // #section-buffer
  }

  @Test
  public void demonstrateBufferAbstractionLeak() {
    // #buffering-abstraction-leak
    final Duration oneSecond = Duration.ofSeconds(1);
    final Source<String, Cancellable> msgSource = Source.tick(oneSecond, oneSecond, "message!");
    final Source<String, Cancellable> tickSource =
        Source.tick(oneSecond.multipliedBy(3), oneSecond.multipliedBy(3), "tick");
    final Flow<String, Integer, NotUsed> conflate =
        Flow.of(String.class).conflateWithSeed(first -> 1, (count, elem) -> count + 1);

    RunnableGraph.fromGraph(
            GraphDSL.create(
                b -> {
                  // this is the asynchronous stage in this graph
                  final FanInShape2<String, Integer, Integer> zipper =
                      b.add(ZipWith.create((String tick, Integer count) -> count).async());
                  b.from(b.add(msgSource)).via(b.add(conflate)).toInlet(zipper.in1());
                  b.from(b.add(tickSource)).toInlet(zipper.in0());
                  b.from(zipper.out()).to(b.add(Sink.foreach(elem -> System.out.println(elem))));
                  return ClosedShape.getInstance();
                }))
        .run(system);
    // #buffering-abstraction-leak
  }

  @Test
  public void demonstrateExplicitBuffers() {
    final Source<Job, NotUsed> inboundJobsConnector = Source.empty();
    // #explicit-buffers-backpressure
    // Getting a stream of jobs from an imaginary external system as a Source
    final Source<Job, NotUsed> jobs = inboundJobsConnector;
    jobs.buffer(1000, OverflowStrategy.backpressure());
    // #explicit-buffers-backpressure

    // #explicit-buffers-droptail
    jobs.buffer(1000, OverflowStrategy.dropTail());
    // #explicit-buffers-droptail

    // #explicit-buffers-dropnew
    jobs.buffer(1000, OverflowStrategy.dropNew());
    // #explicit-buffers-dropnew

    // #explicit-buffers-drophead
    jobs.buffer(1000, OverflowStrategy.dropHead());
    // #explicit-buffers-drophead

    // #explicit-buffers-dropbuffer
    jobs.buffer(1000, OverflowStrategy.dropBuffer());
    // #explicit-buffers-dropbuffer

    // #explicit-buffers-fail
    jobs.buffer(1000, OverflowStrategy.fail());
    // #explicit-buffers-fail
  }
}
