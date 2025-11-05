/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.function.Creator;
import akka.stream.javadsl.*;
import akka.testkit.TestProbe;
import jdocs.AbstractJavaTest;
import jdocs.stream.TwitterStreamQuickstartDocTest.Model.Author;
import jdocs.stream.TwitterStreamQuickstartDocTest.Model.Tweet;
import akka.testkit.javadsl.TestKit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
// #imports
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Processor;
// #imports
import org.reactivestreams.Subscription;

import java.lang.Exception;

import static jdocs.stream.ReactiveStreamsDocTest.Fixture.Data.authors;
import static jdocs.stream.TwitterStreamQuickstartDocTest.Model.AKKA;

public class ReactiveStreamsDocTest extends AbstractJavaTest {

  static ActorSystem system;
  static TestProbe storageProbe;
  static TestProbe alertProbe;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("ReactiveStreamsDocTest");
    storageProbe = new TestProbe(system);
    alertProbe = new TestProbe(system);
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
    storageProbe = null;
    alertProbe = null;
  }

  static class Fixture {
    // below class additionally helps with aligning code includes nicely
    static class Data {

      static // #authors
      final Flow<Tweet, Author, NotUsed> authors =
          Flow.of(Tweet.class).filter(t -> t.hashtags().contains(AKKA)).map(t -> t.author);

      // #authors
    }

    static interface RS {
      // #tweets-publisher
      Publisher<Tweet> tweets();
      // #tweets-publisher

      // #author-storage-subscriber
      Subscriber<Author> storage();
      // #author-storage-subscriber

      // #author-alert-subscriber
      Subscriber<Author> alert();
      // #author-alert-subscriber
    }
  }

  final Fixture.RS rs =
      new Fixture.RS() {
        @Override
        public Publisher<Tweet> tweets() {
          return TwitterStreamQuickstartDocTest.Model.tweets.runWith(
              Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
        }

        /**
         * This is a minimal version of SubscriberProbe, which lives in akka-stream-testkit (test
         * scope) and for now wanted to avoid setting up (test -> compile) dependency for Maven).
         *
         * <p>TODO: Once SubscriberProbe is easily used here replace this MPS with it.
         */
        class MinimalProbeSubscriber<T> implements Subscriber<T> {

          private final ActorRef ref;

          public MinimalProbeSubscriber(ActorRef ref) {
            this.ref = ref;
          }

          @Override
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(T t) {
            ref.tell(t, ActorRef.noSender());
          }

          @Override
          public void onError(Throwable t) {
            ref.tell(t, ActorRef.noSender());
          }

          @Override
          public void onComplete() {
            ref.tell("complete", ActorRef.noSender());
          }
        }

        @Override
        public Subscriber<Author> storage() {
          return new MinimalProbeSubscriber<>(storageProbe.ref());
        }

        @Override
        public Subscriber<Author> alert() {
          return new MinimalProbeSubscriber<>(alertProbe.ref());
        }
      };

  @Test
  public void reactiveStreamsPublisherViaFlowToSubscriber() throws Exception {
    new TestKit(system) {
      final TestProbe probe = new TestProbe(system);

      {
        // #connect-all
        Source.fromPublisher(rs.tweets()).via(authors).to(Sink.fromSubscriber(rs.storage()));
        // #connect-all
      }
    };
  }

  @Test
  public void flowAsPublisherAndSubscriber() throws Exception {
    new TestKit(system) {
      final TestProbe probe = new TestProbe(system);

      {
        // #flow-publisher-subscriber
        final Processor<Tweet, Author> processor = authors.toProcessor().run(system);

        rs.tweets().subscribe(processor);
        processor.subscribe(rs.storage());
        // #flow-publisher-subscriber

        assertStorageResult();
      }
    };
  }

  @Test
  public void sourceAsPublisher() throws Exception {
    new TestKit(system) {
      final TestProbe probe = new TestProbe(system);

      {
        // #source-publisher
        final Publisher<Author> authorPublisher =
            Source.fromPublisher(rs.tweets())
                .via(authors)
                .runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);

        authorPublisher.subscribe(rs.storage());
        // #source-publisher

        assertStorageResult();
      }
    };
  }

  @Test
  public void sourceAsFanoutPublisher() throws Exception {
    new TestKit(system) {
      final TestProbe probe = new TestProbe(system);

      {
        // #source-fanoutPublisher
        final Publisher<Author> authorPublisher =
            Source.fromPublisher(rs.tweets())
                .via(authors)
                .runWith(Sink.asPublisher(AsPublisher.WITH_FANOUT), system);

        authorPublisher.subscribe(rs.storage());
        authorPublisher.subscribe(rs.alert());
        // #source-fanoutPublisher

        assertStorageResult();
      }
    };
  }

  @Test
  public void sinkAsSubscriber() throws Exception {
    new TestKit(system) {
      final TestProbe probe = new TestProbe(system);

      {
        // #sink-subscriber
        final Subscriber<Author> storage = rs.storage();

        final Subscriber<Tweet> tweetSubscriber =
            authors.to(Sink.fromSubscriber(storage)).runWith(Source.asSubscriber(), system);

        rs.tweets().subscribe(tweetSubscriber);
        // #sink-subscriber

        assertStorageResult();
      }
    };
  }

  @Test
  public void useProcessor() throws Exception {
    new TestKit(system) {
      {
        // #use-processor
        // An example Processor factory
        final Creator<Processor<Integer, Integer>> factory =
            new Creator<Processor<Integer, Integer>>() {
              public Processor<Integer, Integer> create() {
                return Flow.of(Integer.class).toProcessor().run(system);
              }
            };

        final Flow<Integer, Integer, NotUsed> flow = Flow.fromProcessor(factory);

        // #use-processor
      }
    };
  }

  void assertStorageResult() {
    storageProbe.expectMsg(new Author("rolandkuhn"));
    storageProbe.expectMsg(new Author("patriknw"));
    storageProbe.expectMsg(new Author("bantonsson"));
    storageProbe.expectMsg(new Author("drewhk"));
    storageProbe.expectMsg(new Author("ktosopl"));
    storageProbe.expectMsg(new Author("mmartynas"));
    storageProbe.expectMsg(new Author("akkateam"));
    storageProbe.expectMsg("complete");
  }
}
