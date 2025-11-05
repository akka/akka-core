/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

import jdocs.AbstractJavaTest;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

// #import
import akka.actor.Actor;
import akka.actor.IndirectActorProducer;
// #import

public class DependencyInjectionDocTest extends AbstractJavaTest {

  public static class TheActor extends AbstractActor {

    final String s;

    public TheActor(String s) {
      this.s = s;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              String.class,
              msg -> {
                getSender().tell(s, getSelf());
              })
          .build();
    }
  }

  static ActorSystem system = null;

  @BeforeClass
  public static void beforeClass() {
    system = ActorSystem.create("DependencyInjectionDocTest");
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  // this is just to make the test below a tiny fraction nicer
  private ActorSystem getContext() {
    return system;
  }

  static
  // #creating-indirectly
  class DependencyInjector implements IndirectActorProducer {
    final Object applicationContext;
    final String beanName;

    public DependencyInjector(Object applicationContext, String beanName) {
      this.applicationContext = applicationContext;
      this.beanName = beanName;
    }

    @Override
    public Class<? extends Actor> actorClass() {
      return TheActor.class;
    }

    @Override
    public TheActor produce() {
      TheActor result;
      // #obtain-fresh-Actor-instance-from-DI-framework
      result = new TheActor((String) applicationContext);
      // #obtain-fresh-Actor-instance-from-DI-framework
      return result;
    }
  }
  // #creating-indirectly

  @Test
  public void indirectActorOf() {
    final String applicationContext = "...";
    // #creating-indirectly

    final ActorRef myActor =
        getContext()
            .actorOf(
                Props.create(DependencyInjector.class, applicationContext, "TheActor"), "TheActor");
    // #creating-indirectly
    new TestKit(system) {
      {
        myActor.tell("hello", getRef());
        expectMsgEquals("...");
      }
    };
  }
}
