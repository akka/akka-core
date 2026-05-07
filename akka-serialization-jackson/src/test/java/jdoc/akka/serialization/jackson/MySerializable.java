/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson;

// #marker-interface

import akka.serialization.jackson.JsonSerializable;

class MyMessage implements JsonSerializable {
  public final String name;
  public final int nr;

  public MyMessage(String name, int nr) {
    this.name = name;
    this.nr = nr;
  }
}
// #marker-interface
