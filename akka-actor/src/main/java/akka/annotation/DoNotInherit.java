/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.annotation;

import java.lang.annotation.*;

/**
 * Marks APIs that are designed under a closed-world assumption for and are NOT meant to be extended
 * by user-code. It is fine to extend these classes within Akka itself, however.
 *
 * <p>This is most useful for binary compatibility purposes when a set of classes and interfaces
 * assume a "closed world" between them, and gain the ability to add methods to the interfaces
 * without breaking binary compatibility for users of this code. Specifically this assumption may be
 * understood intuitively: as all classes that implement this interface are in this compilation unit
 * / artifact, it is impossible to obtain an "old" class with a "new" interface, as they are part of
 * the same dependency.
 *
 * <p>Notable examples of such API include the FlowOps trait in Akka Streams or Akka HTTP model
 * interfaces, which extensively uses inheritance internally, but are not meant for extension by
 * user code.
 */
@Documented
@Retention(RetentionPolicy.CLASS) // to be accessible by MiMa
@Target({ElementType.TYPE})
public @interface DoNotInherit {}
