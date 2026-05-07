/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import docs.stream.operators.sourceorflow.ExtrapolateAndExpandCommon;
import docs.stream.operators.sourceorflow.ExtrapolateAndExpandCommon.Frame;
import java.time.Duration;
import java.util.stream.Stream;

/** */
public class ExtrapolateAndExpand {
  public static Function<ByteString, Frame> decodeAsFrame =
      ExtrapolateAndExpandCommon.Frame$.MODULE$::decode;

  public static Frame BLACK_FRAME = ExtrapolateAndExpandCommon.Frame$.MODULE$.blackFrame();

  public static long nowInSeconds() {
    return ExtrapolateAndExpand.nowInSeconds();
  }

  public static void main(String[] args) {
    ActorSystem actorSystem = ActorSystem.create("25fps-stream");

    Source<ByteString, NotUsed> networkSource = ExtrapolateAndExpandCommon.networkSource().asJava();

    Flow<ByteString, Frame, NotUsed> decode = Flow.of(ByteString.class).<Frame>map(decodeAsFrame);

    // #extrapolate
    // if upstream is too slow, produce copies of the last frame but grayed out.
    Flow<Frame, Frame, NotUsed> rateControl =
        Flow.of(Frame.class)
            .extrapolate(
                lastFrame -> {
                  Frame gray =
                      new Frame(
                          ByteString.fromString(
                              "gray frame!! - " + lastFrame.pixels().utf8String()));
                  return Stream.iterate(gray, i -> i).iterator();
                },
                BLACK_FRAME // initial value
                );

    Source<Frame, NotUsed> videoSource = networkSource.via(decode).via(rateControl);

    // let's create a 25fps stream (a Frame every 40.millis)
    Source<String, Cancellable> tickSource =
        Source.tick(Duration.ZERO, Duration.ofMillis(40), "tick");

    Source<Frame, Cancellable> videoAt25Fps = tickSource.zip(videoSource).map(Pair::second);

    // #extrapolate

    // #expand
    // each element flowing through the stream is expanded to a watermark copy
    // of the upstream frame and grayed out copies. The grayed out copies should
    // only be used downstream if the producer is too slow.
    Flow<Frame, Frame, NotUsed> watermarkerRateControl =
        Flow.of(Frame.class)
            .expand(
                lastFrame -> {
                  Frame watermarked =
                      new Frame(
                          lastFrame.pixels().$plus$plus(ByteString.fromString(" - watermark")));
                  Frame gray =
                      new Frame(lastFrame.pixels().$plus$plus(ByteString.fromString(" - gray")));
                  return Stream.concat(Stream.of(watermarked), Stream.iterate(gray, i -> i))
                      .iterator();
                });

    Source<Frame, NotUsed> watermakedVideoSource =
        networkSource.via(decode).via(watermarkerRateControl);

    // let's create a 25fps stream (a Frame every 40.millis)
    Source<String, Cancellable> ticks = Source.tick(Duration.ZERO, Duration.ofMillis(40), "tick");

    Source<Frame, Cancellable> watermarkedVideoAt25Fps =
        ticks.zip(watermakedVideoSource).map(Pair::second);

    // #expand
    videoAt25Fps
        .map(Frame::pixels)
        .map(ByteString::utf8String)
        .map(pixels -> nowInSeconds() + " - " + pixels)
        .to(Sink.foreach(System.out::println))
        .run(actorSystem);
  }
}
