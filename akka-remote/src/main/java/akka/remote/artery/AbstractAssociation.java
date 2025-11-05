/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery;

import akka.util.Unsafe;

class AbstractAssociation {
  protected static final long sharedStateOffset;

  static {
    try {
      sharedStateOffset =
          Unsafe.instance.objectFieldOffset(
              Association.class.getDeclaredField("_sharedStateDoNotCallMeDirectly"));
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
  }
}
