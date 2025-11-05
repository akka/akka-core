/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon;

import akka.actor.ActorCell;
import akka.util.Unsafe;

final class AbstractActorCell {
  static final long mailboxOffset;
  static final long childrenOffset;
  static final long nextNameOffset;
  static final long functionRefsOffset;

  static {
    try {
      mailboxOffset =
          Unsafe.instance.objectFieldOffset(
              ActorCell.class.getDeclaredField(
                  "akka$actor$dungeon$Dispatch$$_mailboxDoNotCallMeDirectly"));
      childrenOffset =
          Unsafe.instance.objectFieldOffset(
              ActorCell.class.getDeclaredField(
                  "akka$actor$dungeon$Children$$_childrenRefsDoNotCallMeDirectly"));
      nextNameOffset =
          Unsafe.instance.objectFieldOffset(
              ActorCell.class.getDeclaredField(
                  "akka$actor$dungeon$Children$$_nextNameDoNotCallMeDirectly"));
      functionRefsOffset =
          Unsafe.instance.objectFieldOffset(
              ActorCell.class.getDeclaredField(
                  "akka$actor$dungeon$Children$$_functionRefsDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
