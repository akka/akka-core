/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon;

import akka.actor.ActorCell;
import akka.annotation.InternalApi;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/** INTERNAL API */
@InternalApi
final class AbstractActorCell {
  static final VarHandle mailboxHandle;
  static final VarHandle childrenHandle;
  private static final VarHandle nextNameHandle;
  static final VarHandle functionRefsHandle;

  static {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(ActorCell.class, MethodHandles.lookup());
      Field mailboxField =
          ActorCell.class.getDeclaredField(
              "akka$actor$dungeon$Dispatch$$_mailboxDoNotCallMeDirectly");
      mailboxHandle = lookup.unreflectVarHandle(mailboxField);

      Field childrenField =
          ActorCell.class.getDeclaredField(
              "akka$actor$dungeon$Children$$_childrenRefsDoNotCallMeDirectly");
      childrenHandle = lookup.unreflectVarHandle(childrenField);

      Field nextNameField =
          ActorCell.class.getDeclaredField(
              "akka$actor$dungeon$Children$$_nextNameDoNotCallMeDirectly");
      nextNameHandle = lookup.unreflectVarHandle(nextNameField);

      Field functionRefsField =
          ActorCell.class.getDeclaredField(
              "akka$actor$dungeon$Children$$_functionRefsDoNotCallMeDirectly");
      functionRefsHandle = lookup.unreflectVarHandle(functionRefsField);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }

  // Note: manual forwarder to be certain we avoid boxing the long
  static long getAndAddNextName(Children children) {
    return (long) nextNameHandle.getAndAdd(children, 1L);
  }
}
