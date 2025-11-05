/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import akka.util.Unsafe;

final class AbstractActorRef {
  static final long cellOffset;
  static final long lookupOffset;

  static {
    try {
      cellOffset =
          Unsafe.instance.objectFieldOffset(
              RepointableActorRef.class.getDeclaredField("_cellDoNotCallMeDirectly"));
      lookupOffset =
          Unsafe.instance.objectFieldOffset(
              RepointableActorRef.class.getDeclaredField("_lookupDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
