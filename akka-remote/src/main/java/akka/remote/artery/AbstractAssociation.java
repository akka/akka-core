/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class AbstractAssociation {
  protected static final VarHandle sharedStateHandle;

  static {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(Association.class, MethodHandles.lookup());
      sharedStateHandle =
          lookup.unreflectVarHandle(
              Association.class.getDeclaredField("_sharedStateDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
