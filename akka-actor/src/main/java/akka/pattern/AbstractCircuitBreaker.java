/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import akka.util.Unsafe;

class AbstractCircuitBreaker {
  protected static final long stateOffset;
  protected static final long resetTimeoutOffset;

  static {
    try {
      stateOffset =
          Unsafe.instance.objectFieldOffset(
              CircuitBreaker.class.getDeclaredField("_currentStateDoNotCallMeDirectly"));
      resetTimeoutOffset =
          Unsafe.instance.objectFieldOffset(
              CircuitBreaker.class.getDeclaredField("_currentResetTimeoutDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
