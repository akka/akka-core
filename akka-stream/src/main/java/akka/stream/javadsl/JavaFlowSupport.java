/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.NotUsed;
import akka.japi.Creator;
import akka.japi.Pair;
import akka.stream.impl.JavaFlowAndRsConverters;

/**
 * For use only with `JDK 9+`.
 *
 * <p>Provides support for `java.util.concurrent.Flow.*` interfaces which mirror the Reactive
 * Streams interfaces from `org.reactivestreams`. See <a
 * href="http//www.reactive-streams.org/">reactive-streams.org</a>.
 */
public final class JavaFlowSupport {

  private static final NotUsed NotUsed = akka.NotUsed.getInstance();

  private JavaFlowSupport() {
    throw new RuntimeException("No instances allowed!");
  }

  /**
   * {@link akka.stream.javadsl.Flow]] factories operating with {@code java.util.concurrent.Flow.*} interfaces.
   */
  public static final class Source {
    private Source() {
      throw new RuntimeException("No instances allowed!");
    }

    /**
     * Helper to create {@code Source} from {@link java.util.concurrent.Flow.Publisher}.
     *
     * <p>Construct a transformation starting with given publisher. The transformation steps are
     * executed by a series of {@link java.util.concurrent.Flow.Processor} instances that mediate
     * the flow of elements downstream and the propagation of back-pressure upstream.
     *
     * <p>See also {@code Source.fromPublisher} if wanting to integrate with {@link
     * org.reactivestreams.Publisher} instead (which carries the same semantics, however existed
     * before RS's inclusion in Java 9).
     */
    public static <T> akka.stream.javadsl.Source<T, NotUsed> fromPublisher(
        java.util.concurrent.Flow.Publisher<T> publisher) {
      return akka.stream.javadsl.Source.<T>fromPublisher(JavaFlowAndRsConverters.asRs(publisher));
    }

    /**
     * Creates a {@code Source} that is materialized as a {@link
     * java.util.concurrent.Flow.Subscriber}.
     *
     * <p>See also {@code Source.asSubscriber} if wanting to integrate with {@link
     * org.reactivestreams.Subscriber} instead (which carries the same semantics, however existed
     * before RS's inclusion in Java 9).
     */
    // #asSubscriber
    public static <T>
        akka.stream.javadsl.Source<T, java.util.concurrent.Flow.Subscriber<T>> asSubscriber() {
      // #asSubscriber
      return akka.stream.javadsl.Source.<T>asSubscriber()
          .mapMaterializedValue(JavaFlowAndRsConverters::asJava);
    }
  }

  /**
   * {@link akka.stream.javadsl.Flow]] factories operating with {@code java.util.concurrent.Flow.*} interfaces.
   */
  public static final class Flow {
    private Flow() {
      throw new RuntimeException("No instances allowed!");
    }

    /** Creates a Flow from a {@link java.util.concurrent.Flow.Processor} */
    public static <I, O> akka.stream.javadsl.Flow<I, O, NotUsed> fromProcessor(
        Creator<java.util.concurrent.Flow.Processor<I, O>> processorFactory) throws Exception {
      return fromProcessorMat(() -> Pair.apply(processorFactory.create(), NotUsed));
    }

    /**
     * Creates a Flow from a {@link java.util.concurrent.Flow.Processor>> and returns a materialized value.
     */
    public static <I, O, M> akka.stream.javadsl.Flow<I, O, M> fromProcessorMat(
        akka.japi.Creator<akka.japi.Pair<java.util.concurrent.Flow.Processor<I, O>, M>>
            processorFactory)
        throws Exception {
      final Pair<java.util.concurrent.Flow.Processor<I, O>, M> value = processorFactory.create();
      final java.util.concurrent.Flow.Processor<I, O> processor = value.first();
      final M mat = value.second();

      return akka.stream.javadsl.Flow.fromProcessorMat(
          () -> akka.japi.Pair.apply(JavaFlowAndRsConverters.asRs(processor), mat));
    }

    /**
     * Converts this Flow to a {@code RunnableGraph} that materializes to a Reactive Streams {@link
     * java.util.concurrent.Flow.Processor} which implements the operations encapsulated by this
     * Flow. Every materialization results in a new Processor instance, i.e. the returned {@code
     * RunnableGraph} is reusable.
     *
     * @return A {@code RunnableGraph} that materializes to a {@code Processor} when {@code run()}
     *     is called on it.
     */
    public static <In, Out, Mat>
        akka.stream.javadsl.RunnableGraph<java.util.concurrent.Flow.Processor<In, Out>> toProcessor(
            akka.stream.javadsl.Flow<In, Out, Mat> flow) {
      final akka.stream.javadsl.Source<In, java.util.concurrent.Flow.Subscriber<In>> source =
          JavaFlowSupport.Source.<In>asSubscriber();
      final akka.stream.javadsl.Sink<Out, java.util.concurrent.Flow.Publisher<Out>> sink =
          JavaFlowSupport.Sink.<Out>asPublisher(AsPublisher.WITHOUT_FANOUT);

      // have to jump though scaladsl for the toMat because type inference of the Keep.both
      return source
          .via(flow)
          .toMat(sink, Keep.both())
          .mapMaterializedValue(
              pair -> {
                final java.util.concurrent.Flow.Subscriber<In> sub = pair.first();
                final java.util.concurrent.Flow.Publisher<Out> pub = pair.second();

                return new java.util.concurrent.Flow.Processor<In, Out>() {
                  @Override
                  public void onError(Throwable t) {
                    sub.onError(t);
                  }

                  @Override
                  public void onSubscribe(java.util.concurrent.Flow.Subscription s) {
                    sub.onSubscribe(s);
                  }

                  @Override
                  public void onComplete() {
                    sub.onComplete();
                  }

                  @Override
                  public void onNext(In t) {
                    sub.onNext(t);
                  }

                  @Override
                  public void subscribe(java.util.concurrent.Flow.Subscriber<? super Out> s) {
                    pub.subscribe(s);
                  }
                };
              });
    }
  }

  /**
   * {@link akka.stream.javadsl.Sink} factories operating with {@code java.util.concurrent.Flow.*}
   * interfaces.
   */
  public static final class Sink {
    private Sink() {
      throw new RuntimeException("No instances allowed!");
    }

    /**
     * A `Sink` that materializes into a {@link java.util.concurrent.Flow.Publisher}.
     *
     * <p>If {@code fanout} is {@code WITH_FANOUT}, the materialized {@code Publisher} will support
     * multiple {@code Subscriber}s and the size of the {@code inputBuffer} configured for this
     * operator becomes the maximum number of elements that the fastest {@link
     * java.util.concurrent.Flow.Subscriber} can be ahead of the slowest one before slowing the
     * processing down due to back pressure.
     *
     * <p>If {@code fanout} is {@code WITHOUT_FANOUT} then the materialized {@code Publisher} will
     * only support a single {@code Subscriber} and reject any additional {@code Subscriber}s.
     */
    public static <T>
        akka.stream.javadsl.Sink<T, java.util.concurrent.Flow.Publisher<T>> asPublisher(
            AsPublisher fanout) {
      return akka.stream.javadsl.Sink.<T>asPublisher(fanout)
          .mapMaterializedValue(JavaFlowAndRsConverters::asJava);
    }

    /** Helper to create <<Sink>> from <<java.util.concurrent.Flow.Subscriber>>. */
    public static <T> akka.stream.javadsl.Sink<T, NotUsed> fromSubscriber(
        java.util.concurrent.Flow.Subscriber<T> s) {
      return akka.stream.javadsl.Sink.fromSubscriber(JavaFlowAndRsConverters.asRs(s));
    }
  }
}
