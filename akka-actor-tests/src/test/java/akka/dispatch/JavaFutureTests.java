/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.actor.ActorSystem;

import akka.japi.*;
import org.junit.ClassRule;
import org.scalatestplus.junit.JUnitSuite;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Duration;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.LinkedList;
import java.lang.Iterable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static akka.japi.Util.classTag;

import akka.testkit.AkkaSpec;

public class JavaFutureTests extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
    new AkkaJUnitActorSystemResource("JavaFutureTests", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();
  private final Duration timeout = Duration.create(5, TimeUnit.SECONDS);

  @Test
  public void mustBeAbleToMapAFuture() throws Exception {

    Future<String> f1 = Futures.future(new Callable<String>() {
      public String call() {
        return "Hello";
      }
    }, system.dispatcher());

    Future<String> f2 = f1.map(new Mapper<String, String>() {
      public String apply(String s) {
        return s + " World";
      }
    }, system.dispatcher());

    assertEquals("Hello World", Await.result(f2, timeout));
  }

  @Test
  public void mustBeAbleToExecuteAnOnResultCallback() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    Future<String> f = cf.future();
    f.onComplete(new OnComplete<String>() {
      public void onComplete(Throwable t, String r) {
        if ("foo".equals(r))
          latch.countDown();
      }
    }, system.dispatcher());

    cf.success("foo");
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals("foo", Await.result(f, timeout));
  }

  @Test
  public void mustBeAbleToExecuteAnOnExceptionCallback() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    Future<String> f = cf.future();
    f.onComplete(new OnComplete<String>() {
      public void onComplete(Throwable t, String r) {
        // 'null instanceof ...' is always false
        if (t instanceof NullPointerException)
          latch.countDown();
      }
    }, system.dispatcher());

    Throwable exception = new NullPointerException();
    cf.failure(exception);
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(exception, f.value().get().failed().get());
  }

  @Test
  public void mustBeAbleToExecuteAnOnCompleteCallback() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    Future<String> f = cf.future();
    f.onComplete(new OnComplete<String>() {
      public void onComplete(Throwable t, String r) {
        latch.countDown();
      }
    }, system.dispatcher());

    cf.success("foo");
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals("foo", Await.result(f, timeout));
  }

  @Test
  public void mustBeAbleToForeachAFuture() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    Future<String> f = cf.future();
    f.foreach(new Foreach<String>() {
      public void each(String future) {
        latch.countDown();
      }
    },system.dispatcher());

    cf.success("foo");
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals("foo", Await.result(f, timeout));
  }

  @Test
  public void mustBeAbleToFlatMapAFuture() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    cf.success("1000");
    Future<String> f = cf.future();
    Future<Integer> r = f.flatMap(new Mapper<String, Future<Integer>>() {
      public Future<Integer> checkedApply(String r) throws Throwable {
        if (false) throw new IOException("Just here to make sure this compiles.");
        latch.countDown();
        Promise<Integer> cf = Futures.promise();
        cf.success(Integer.parseInt(r));
        return cf.future();
      }
    }, system.dispatcher());

    assertEquals("1000", Await.result(f, timeout));
    assertEquals(1000, Await.result(r, timeout).intValue());
    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToFilterAFuture() throws Throwable {
    final CountDownLatch latch = new CountDownLatch(1);
    Promise<String> cf = Futures.promise();
    Future<String> f = cf.future();
    Future<String> r = f.filter(Filter.filterOf(new Function<String, Boolean>() {
      public Boolean apply(String r) {
        latch.countDown();
        return r.equals("foo");
      }
    }), system.dispatcher());

    cf.success("foo");
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals("foo", Await.result(f, timeout));
    assertEquals("foo", Await.result(r, timeout));
  }

  // TODO: Improve this test, perhaps with an Actor
  @Test
  public void mustSequenceAFutureList() throws Exception{
    LinkedList<Future<String>> listFutures = new LinkedList<>();
    LinkedList<String> listExpected = new LinkedList<>();

    for (int i = 0; i < 10; i++) {
      listExpected.add("test");
      listFutures.add(Futures.future(new Callable<String>() {
        public String call() {
          return "test";
        }
      }, system.dispatcher()));
    }

    Future<Iterable<String>> futureList = Futures.sequence(listFutures, system.dispatcher());

    assertEquals(listExpected, Await.result(futureList, timeout));
  }

  // TODO: Improve this test, perhaps with an Actor
  @Test
  public void foldForJavaApiMustWork() throws Exception{
    LinkedList<Future<String>> listFutures = new LinkedList<>();
    StringBuilder expected = new StringBuilder();

    for (int i = 0; i < 10; i++) {
      expected.append("test");
      listFutures.add(Futures.future(new Callable<String>() {
        public String call() {
          return "test";
        }
      }, system.dispatcher()));
    }

    Future<String> result = Futures.fold("", listFutures, new Function2<String, String, String>() {
      public String apply(String r, String t) {
        return r + t;
      }
    }, system.dispatcher());

    assertEquals(expected.toString(), Await.result(result, timeout));
  }

  @Test
  public void reduceForJavaApiMustWork() throws Exception{
    LinkedList<Future<String>> listFutures = new LinkedList<>();
    StringBuilder expected = new StringBuilder();

    for (int i = 0; i < 10; i++) {
      expected.append("test");
      listFutures.add(Futures.future(new Callable<String>() {
        public String call() {
          return "test";
        }
      }, system.dispatcher()));
    }

    Future<String> result = Futures.reduce(listFutures, new Function2<String, String, String>() {
      public String apply(String r, String t) {
        return r + t;
      }
    }, system.dispatcher());

    assertEquals(expected.toString(), Await.result(result, timeout));
  }

  @Test
  public void traverseForJavaApiMustWork() throws Exception{
    LinkedList<String> listStrings = new LinkedList<>();
    LinkedList<String> expectedStrings = new LinkedList<>();

    for (int i = 0; i < 10; i++) {
      expectedStrings.add("TEST");
      listStrings.add("test");
    }

    Future<Iterable<String>> result = Futures.traverse(listStrings, new Function<String, Future<String>>() {
      public Future<String> apply(final String r) {
        return Futures.future(new Callable<String>() {
          public String call() {
            return r.toUpperCase();
          }
        }, system.dispatcher());
      }
    }, system.dispatcher());

    assertEquals(expectedStrings, Await.result(result, timeout));
  }

  @Test
  public void findForJavaApiMustWork() throws Exception{
    LinkedList<Future<Integer>> listFutures = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      final Integer fi = i;
      listFutures.add(Futures.future(new Callable<Integer>() {
        public Integer call() {
          return fi;
        }
      }, system.dispatcher()));
    }
    final Integer expect = 5;
    Future<Option<Integer>> f = Futures.find(listFutures, new Function<Integer, Boolean>() {
      public Boolean apply(Integer i) {
        return i == 5;
      }
    }, system.dispatcher());

    assertEquals(expect, Await.result(f, timeout).get());
  }

  @Test
  public void blockMustBeCallable() throws Exception {
    Promise<String> p = Futures.promise();
    Duration d = Duration.create(1, TimeUnit.SECONDS);
    p.success("foo");
    Await.ready(p.future(), d);
    assertEquals("foo", Await.result(p.future(), d));
  }

  @Test
  public void mapToMustBeCallable() throws Exception {
    Promise<Object> p = Futures.promise();
    Future<String> f = p.future().mapTo(classTag(String.class));
    Duration d = Duration.create(1, TimeUnit.SECONDS);
    p.success("foo");
    Await.ready(p.future(), d);
    assertEquals("foo", Await.result(p.future(), d));
  }

  @Test
  public void recoverToMustBeCallable() throws Exception {
    final IllegalStateException fail = new IllegalStateException("OHNOES");
    Promise<Object> p = Futures.promise();
    Future<Object> f = p.future().recover(new Recover<Object>() {
      public Object recover(Throwable t) throws Throwable {
        if (t == fail)
          return "foo";
        else
          throw t;
      }
    }, system.dispatcher());
    Duration d = Duration.create(1, TimeUnit.SECONDS);
    p.failure(fail);
    assertEquals("foo", Await.result(f, d));
  }

  @Test
  public void recoverWithToMustBeCallable() throws Exception{
    final IllegalStateException fail = new IllegalStateException("OHNOES");
    Promise<Object> p = Futures.promise();
    Future<Object> f = p.future().recoverWith(new Recover<Future<Object>>() {
      public Future<Object> recover(Throwable t) throws Throwable {
        if (t == fail)
          return Futures.successful("foo");
        else
          throw t;
      }
    }, system.dispatcher());
    Duration d = Duration.create(1, TimeUnit.SECONDS);
    p.failure(fail);
    assertEquals("foo", Await.result(f, d));
  }
}
