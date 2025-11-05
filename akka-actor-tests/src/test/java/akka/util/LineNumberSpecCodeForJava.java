/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util;

import java.io.Serializable;

/*
 * IMPORTANT: do not change this file, the line numbers are verified in LineNumberSpec
 */

public class LineNumberSpecCodeForJava {

  public static interface F extends Serializable {
    public String doit(String arg);
  }

  public F f1() {
    return (s) -> s;
  }

  public F f2() {
    return (s) -> {
      System.out.println(s);
      return s;
    };
  }

  public F f3() {
    return new F() {
      private static final long serialVersionUID = 1L;

      @Override
      public String doit(String arg) {
        return arg;
      }
    };
  }
}
