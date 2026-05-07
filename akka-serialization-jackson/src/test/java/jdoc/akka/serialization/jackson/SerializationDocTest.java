/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson;

import akka.serialization.jackson.JsonSerializable;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class SerializationDocTest {

  interface OneConstructorParamExample1 {

    // #one-constructor-param-1
    public class SimpleCommand implements JsonSerializable {
      private final String name;

      public SimpleCommand(String name) {
        this.name = name;
      }
    }
    // #one-constructor-param-1
  }

  interface OneConstructorParamExample2 {
    // #one-constructor-param-2
    public class SimpleCommand implements JsonSerializable {
      private final String name;

      public SimpleCommand(String name) {
        this.name = name;
      }
    }
    // #one-constructor-param-2
  }

  interface OneConstructorParamExample3 {
    // #one-constructor-param-3
    public class SimpleCommand implements JsonSerializable {
      private final String name;

      public SimpleCommand(@JsonProperty("name") String name) {
        this.name = name;
      }
    }
    // #one-constructor-param-3
  }

  interface Polymorphism {
    // #polymorphism
    public class Zoo implements JsonSerializable {
      public final Animal primaryAttraction;

      public Zoo(Animal primaryAttraction) {
        this.primaryAttraction = primaryAttraction;
      }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
      @JsonSubTypes.Type(value = Lion.class, name = "lion"),
      @JsonSubTypes.Type(value = Elephant.class, name = "elephant")
    })
    interface Animal {}

    public final class Lion implements Animal {
      public final String name;

      public Lion(String name) {
        this.name = name;
      }
    }

    public final class Elephant implements Animal {
      public final String name;
      public final int age;

      public Elephant(String name, int age) {
        this.name = name;
        this.age = age;
      }
    }
    // #polymorphism
  }
}
