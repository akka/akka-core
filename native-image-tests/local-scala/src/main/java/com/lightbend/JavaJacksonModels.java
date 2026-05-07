/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend;

import akka.serialization.jackson.JsonSerializable;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

public class JavaJacksonModels {

    public static class SimpleCommand implements JsonSerializable {
        private final String name;
        public SimpleCommand(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimpleCommand that = (SimpleCommand) o;

            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    public static class Zoo implements JsonSerializable {
        public final Animal primaryAttraction;

        public Zoo(Animal primaryAttraction) {
            this.primaryAttraction = primaryAttraction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Zoo zoo = (Zoo) o;

            return Objects.equals(primaryAttraction, zoo.primaryAttraction);
        }

        @Override
        public int hashCode() {
            return primaryAttraction != null ? primaryAttraction.hashCode() : 0;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Lion.class, name = "lion"),
            @JsonSubTypes.Type(value = Elephant.class, name = "elephant")
    })
    interface Animal {}

    public static final class Lion implements Animal {
        public final String name;

        public Lion(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Lion lion = (Lion) o;

            return Objects.equals(name, lion.name);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    public static final class Elephant implements Animal {
        public final String name;
        public final int age;

        public Elephant(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Elephant elephant = (Elephant) o;

            if (age != elephant.age) return false;
            return Objects.equals(name, elephant.name);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            return result;
        }
    }
}
