/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
public @interface TestAnnotation {
  String someString() default "pigdog";
}
