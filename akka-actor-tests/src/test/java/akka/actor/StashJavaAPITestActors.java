/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static org.junit.Assert.*;

public class StashJavaAPITestActors {

  /*
   * Helper method to make the tests of AbstractActorWithStash, AbstractActorWithUnboundedStash and
   * AbstractActorWithUnrestrictedStash more DRY since mixin is not possible.
   */
  private static int testReceive(
      Object msg, int count, ActorRef sender, ActorRef self, UnrestrictedStash stash) {
    if (msg instanceof String) {
      if (count < 0) {
        sender.tell(((String) msg).length(), self);
      } else if (count == 2) {
        stash.unstashAll();
        return -1;
      } else {
        stash.stash();
        return count + 1;
      }
    } else if (msg instanceof Integer) {
      int value = (Integer) msg;
      assertEquals(5, value);
    }
    return count;
  }

  public static class WithStash extends AbstractActorWithStash {
    int count = 0;

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Object.class,
              msg -> {
                count = testReceive(msg, count, getSender(), getSelf(), this);
              })
          .build();
    }
  }

  public static class WithUnboundedStash extends AbstractActorWithUnboundedStash {
    int count = 0;

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Object.class,
              msg -> {
                count = testReceive(msg, count, getSender(), getSelf(), this);
              })
          .build();
    }
  }

  public static class WithUnrestrictedStash extends AbstractActorWithUnrestrictedStash {
    int count = 0;

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Object.class,
              msg -> {
                count = testReceive(msg, count, getSender(), getSelf(), this);
              })
          .build();
    }
  }
}
