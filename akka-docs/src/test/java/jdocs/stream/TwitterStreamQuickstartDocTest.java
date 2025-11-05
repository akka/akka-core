/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.JavaPartialFunction;
// #imports
import akka.stream.*;
import akka.stream.javadsl.*;
// #imports
import jdocs.AbstractJavaTest;
import jdocs.stream.TwitterStreamQuickstartDocTest.Model.Author;
import jdocs.stream.TwitterStreamQuickstartDocTest.Model.Hashtag;
import jdocs.stream.TwitterStreamQuickstartDocTest.Model.Tweet;
import akka.testkit.javadsl.TestKit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static jdocs.stream.TwitterStreamQuickstartDocTest.Model.AKKA;
import static jdocs.stream.TwitterStreamQuickstartDocTest.Model.tweets;

@SuppressWarnings("unused")
public class TwitterStreamQuickstartDocTest extends AbstractJavaTest {

  private static final long serialVersionUID = 1L;

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("TwitterStreamQuickstartDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  abstract static class Model {
    // #model
    public static class Author {
      public final String handle;

      public Author(String handle) {
        this.handle = handle;
      }

      // ...

      // #model

      @Override
      public String toString() {
        return "Author(" + handle + ")";
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        Author author = (Author) o;

        if (handle != null ? !handle.equals(author.handle) : author.handle != null) {
          return false;
        }

        return true;
      }

      @Override
      public int hashCode() {
        return handle != null ? handle.hashCode() : 0;
      }
      // #model
    }
    // #model

    // #model

    public static class Hashtag {
      public final String name;

      public Hashtag(String name) {
        this.name = name;
      }

      // ...
      // #model

      @Override
      public int hashCode() {
        return name.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Hashtag other = (Hashtag) obj;
        return name.equals(other.name);
      }

      @Override
      public String toString() {
        return "Hashtag(" + name + ")";
      }
      // #model
    }
    // #model

    // #model

    public static class Tweet {
      public final Author author;
      public final long timestamp;
      public final String body;

      public Tweet(Author author, long timestamp, String body) {
        this.author = author;
        this.timestamp = timestamp;
        this.body = body;
      }

      public Set<Hashtag> hashtags() {
        return Arrays.asList(body.split(" ")).stream()
            .filter(a -> a.startsWith("#"))
            .map(a -> new Hashtag(a))
            .collect(Collectors.toSet());
      }

      // ...
      // #model

      @Override
      public String toString() {
        return "Tweet(" + author + "," + timestamp + "," + body + ")";
      }

      // #model
    }
    // #model

    // #model

    public static final Hashtag AKKA = new Hashtag("#akka");
    // #model

    public static final Source<Tweet, NotUsed> tweets =
        Source.from(
            Arrays.asList(
                new Tweet[] {
                  new Tweet(new Author("rolandkuhn"), System.currentTimeMillis(), "#akka rocks!"),
                  new Tweet(new Author("patriknw"), System.currentTimeMillis(), "#akka !"),
                  new Tweet(new Author("bantonsson"), System.currentTimeMillis(), "#akka !"),
                  new Tweet(new Author("drewhk"), System.currentTimeMillis(), "#akka !"),
                  new Tweet(
                      new Author("ktosopl"), System.currentTimeMillis(), "#akka on the rocks!"),
                  new Tweet(new Author("mmartynas"), System.currentTimeMillis(), "wow #akka !"),
                  new Tweet(new Author("akkateam"), System.currentTimeMillis(), "#akka rocks!"),
                  new Tweet(new Author("bananaman"), System.currentTimeMillis(), "#bananas rock!"),
                  new Tweet(new Author("appleman"), System.currentTimeMillis(), "#apples rock!"),
                  new Tweet(
                      new Author("drama"),
                      System.currentTimeMillis(),
                      "we compared #apples to #oranges!")
                }));
  }

  abstract static class Example0 {
    // #tweet-source
    Source<Tweet, NotUsed> tweets;
    // #tweet-source
  }

  abstract static class Example1 {
    // #first-sample
    // #system-setup
    final ActorSystem system = ActorSystem.create("reactive-tweets");
    // #first-sample
    // #system-setup
  }

  static class Example2 {
    public void run(final Materializer mat)
        throws TimeoutException, InterruptedException, ExecutionException {
      // #backpressure-by-readline
      final CompletionStage<Done> completion =
          Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
              .map(
                  i -> {
                    System.out.println("map => " + i);
                    return i;
                  })
              .runForeach(
                  i ->
                      System.console()
                          .readLine("Element = %s continue reading? [press enter]\n", i),
                  mat);

      completion.toCompletableFuture().get(1, TimeUnit.SECONDS);
      // #backpressure-by-readline
    }
  }

  @Test
  public void demonstrateFilterAndMap() {
    final SilenceSystemOut.System System = SilenceSystemOut.get();

    // #first-sample

    // #authors-filter-map
    final Source<Author, NotUsed> authors =
        tweets.filter(t -> t.hashtags().contains(AKKA)).map(t -> t.author);
    // #first-sample
    // #authors-filter-map

    new Object() {
      // #authors-collect
      JavaPartialFunction<Tweet, Author> collectFunction =
          new JavaPartialFunction<Tweet, Author>() {
            public Author apply(Tweet t, boolean isCheck) {
              if (t.hashtags().contains(AKKA)) {
                if (isCheck) return null; // to spare the expensive or side-effecting code
                return t.author;
              } else {
                throw noMatch();
              }
            }
          };

      final Source<Author, NotUsed> authors = tweets.collect(collectFunction);
      // #authors-collect
    };

    // #first-sample

    // #authors-foreachsink-println
    authors.runWith(Sink.foreach(a -> System.out.println(a)), system);
    // #first-sample
    // #authors-foreachsink-println

    // #authors-foreach-println
    authors.runForeach(a -> System.out.println(a), system);
    // #authors-foreach-println
  }

  @Test
  public void demonstrateMapConcat() {
    // #hashtags-mapConcat
    final Source<Hashtag, NotUsed> hashtags =
        tweets.mapConcat(t -> new ArrayList<Hashtag>(t.hashtags()));
    // #hashtags-mapConcat
  }

  abstract static class HiddenDefinitions {
    // #graph-dsl-broadcast
    Sink<Author, NotUsed> writeAuthors;
    Sink<Hashtag, NotUsed> writeHashtags;
    // #graph-dsl-broadcast
  }

  @Test
  public void demonstrateBroadcast() {
    final Sink<Author, CompletionStage<Done>> writeAuthors = Sink.ignore();
    final Sink<Hashtag, CompletionStage<Done>> writeHashtags = Sink.ignore();

    // #graph-dsl-broadcast
    RunnableGraph.fromGraph(
            GraphDSL.create(
                b -> {
                  final UniformFanOutShape<Tweet, Tweet> bcast = b.add(Broadcast.create(2));
                  final FlowShape<Tweet, Author> toAuthor =
                      b.add(Flow.of(Tweet.class).map(t -> t.author));
                  final FlowShape<Tweet, Hashtag> toTags =
                      b.add(
                          Flow.of(Tweet.class)
                              .mapConcat(t -> new ArrayList<Hashtag>(t.hashtags())));
                  final SinkShape<Author> authors = b.add(writeAuthors);
                  final SinkShape<Hashtag> hashtags = b.add(writeHashtags);

                  b.from(b.add(tweets)).viaFanOut(bcast).via(toAuthor).to(authors);
                  b.from(bcast).via(toTags).to(hashtags);
                  return ClosedShape.getInstance();
                }))
        .run(system);
    // #graph-dsl-broadcast
  }

  long slowComputation(Tweet t) {
    try {
      // act as if performing some heavy computation
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }
    return 42;
  }

  @Test
  public void demonstrateSlowProcessing() {
    // #tweets-slow-consumption-dropHead
    tweets
        .buffer(10, OverflowStrategy.dropHead())
        .map(t -> slowComputation(t))
        .runWith(Sink.ignore(), system);
    // #tweets-slow-consumption-dropHead
  }

  @Test
  public void demonstrateCountOnFiniteStream() {
    // #tweets-fold-count
    final Sink<Integer, CompletionStage<Integer>> sumSink =
        Sink.<Integer, Integer>fold(0, (acc, elem) -> acc + elem);

    final RunnableGraph<CompletionStage<Integer>> counter =
        tweets.map(t -> 1).toMat(sumSink, Keep.right());

    final CompletionStage<Integer> sum = counter.run(system);

    sum.thenAcceptAsync(
        c -> System.out.println("Total tweets processed: " + c), system.dispatcher());
    // #tweets-fold-count

    new Object() {
      // #tweets-fold-count-oneline
      final CompletionStage<Integer> sum = tweets.map(t -> 1).runWith(sumSink, system);
      // #tweets-fold-count-oneline
    };
  }

  @Test
  public void demonstrateMaterializeMultipleTimes() {
    final Source<Tweet, NotUsed> tweetsInMinuteFromNow =
        tweets; // not really in second, just acting as if

    // #tweets-runnable-flow-materialized-twice
    final Sink<Integer, CompletionStage<Integer>> sumSink =
        Sink.<Integer, Integer>fold(0, (acc, elem) -> acc + elem);
    final RunnableGraph<CompletionStage<Integer>> counterRunnableGraph =
        tweetsInMinuteFromNow
            .filter(t -> t.hashtags().contains(AKKA))
            .map(t -> 1)
            .toMat(sumSink, Keep.right());

    // materialize the stream once in the morning
    final CompletionStage<Integer> morningTweetsCount = counterRunnableGraph.run(system);
    // and once in the evening, reusing the blueprint
    final CompletionStage<Integer> eveningTweetsCount = counterRunnableGraph.run(system);
    // #tweets-runnable-flow-materialized-twice

  }
}
