/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

class AbstractCircuitBreaker {
  protected static final VarHandle currentStateHandle;
  protected static final VarHandle resetTimeoutHandle;

  static {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(CircuitBreaker.class, MethodHandles.lookup());
      Field currentStateField =
          CircuitBreaker.class.getDeclaredField("_currentStateDoNotCallMeDirectly");
      currentStateHandle = lookup.unreflectVarHandle(currentStateField);

      Field resetTimeoutField =
          CircuitBreaker.class.getDeclaredField("_currentResetTimeoutDoNotCallMeDirectly");
      resetTimeoutHandle = lookup.unreflectVarHandle(resetTimeoutField);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
