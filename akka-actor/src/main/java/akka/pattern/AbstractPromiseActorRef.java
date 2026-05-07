/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

final class AbstractPromiseActorRef {
  static final VarHandle stateHandle;
  static final VarHandle watchedByHandle;

  static {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(PromiseActorRef.class, MethodHandles.lookup());
      Field stateField = PromiseActorRef.class.getDeclaredField("_stateDoNotCallMeDirectly");
      stateHandle = lookup.unreflectVarHandle(stateField);

      Field watchedByField =
          PromiseActorRef.class.getDeclaredField("_watchedByDoNotCallMeDirectly");
      watchedByHandle = lookup.unreflectVarHandle(watchedByField);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
