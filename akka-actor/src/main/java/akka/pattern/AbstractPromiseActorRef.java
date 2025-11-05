/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import akka.util.Unsafe;

final class AbstractPromiseActorRef {
  static final long stateOffset;
  static final long watchedByOffset;

  static {
    try {
      stateOffset =
          Unsafe.instance.objectFieldOffset(
              PromiseActorRef.class.getDeclaredField("_stateDoNotCallMeDirectly"));
      watchedByOffset =
          Unsafe.instance.objectFieldOffset(
              PromiseActorRef.class.getDeclaredField("_watchedByDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
