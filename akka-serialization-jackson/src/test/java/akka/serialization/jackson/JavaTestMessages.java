/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson;

import akka.actor.ActorRef;
import akka.actor.Address;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface JavaTestMessages {

  public interface TestMessage {}

  public class SimpleCommand implements TestMessage {
    private final String name;

    public SimpleCommand(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SimpleCommand that = (SimpleCommand) o;
      return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }

  public class SimpleCommand2 implements TestMessage {
    public final String name;
    public final String name2;

    // note that no annotation needed here, `javac -parameters` and not single param constructor
    public SimpleCommand2(String name, String name2) {
      this.name = name;
      this.name2 = name2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleCommand2 that = (SimpleCommand2) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      return name2 != null ? name2.equals(that.name2) : that.name2 == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (name2 != null ? name2.hashCode() : 0);
      return result;
    }
  }

  public class OptionalCommand implements TestMessage {
    private final Optional<String> maybe;

    public OptionalCommand(@JsonProperty("maybe") Optional<String> maybe) {
      this.maybe = maybe;
    }

    public Optional<String> getMaybe() {
      return maybe;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      OptionalCommand that = (OptionalCommand) o;

      return maybe != null ? maybe.equals(that.maybe) : that.maybe == null;
    }

    @Override
    public int hashCode() {
      return maybe != null ? maybe.hashCode() : 0;
    }
  }

  public class BooleanCommand implements TestMessage {
    private final boolean published;

    public BooleanCommand(@JsonProperty("published") boolean published) {
      this.published = published;
    }

    public boolean isPublished() {
      return published;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BooleanCommand that = (BooleanCommand) o;

      return published == that.published;
    }

    @Override
    public int hashCode() {
      return (published ? 1 : 0);
    }
  }

  public class CollectionsCommand implements TestMessage {
    private final List<String> strings;
    // if this was List<Object> it would not automatically work,
    // which is good, otherwise arbitrary classes could be loaded
    private final List<SimpleCommand> objects;

    public CollectionsCommand(List<String> strings, List<SimpleCommand> objects) {
      this.strings = strings;
      this.objects = objects;
    }

    public List<String> getStrings() {
      return strings;
    }

    public List<SimpleCommand> getObjects() {
      return objects;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CollectionsCommand that = (CollectionsCommand) o;

      if (strings != null ? !strings.equals(that.strings) : that.strings != null) return false;
      return objects != null ? objects.equals(that.objects) : that.objects == null;
    }

    @Override
    public int hashCode() {
      int result = strings != null ? strings.hashCode() : 0;
      result = 31 * result + (objects != null ? objects.hashCode() : 0);
      return result;
    }
  }

  public class TimeCommand implements TestMessage {
    public final LocalDateTime timestamp;
    public final Duration duration;

    public TimeCommand(LocalDateTime timestamp, Duration duration) {
      this.timestamp = timestamp;
      this.duration = duration;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TimeCommand that = (TimeCommand) o;

      if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
        return false;
      return duration != null ? duration.equals(that.duration) : that.duration == null;
    }

    @Override
    public int hashCode() {
      int result = timestamp != null ? timestamp.hashCode() : 0;
      result = 31 * result + (duration != null ? duration.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "TimeCommand{" + "timestamp=" + timestamp + ", duration=" + duration + '}';
    }
  }

  public class InstantCommand implements TestMessage {
    public final Instant instant;

    public InstantCommand(Instant instant) {
      this.instant = instant;
    }

    public Instant getInstant() {
      return instant;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InstantCommand that = (InstantCommand) o;

      return instant.equals(that.instant);
    }

    @Override
    public int hashCode() {
      return instant.hashCode();
    }

    @Override
    public String toString() {
      return "InstantCommand{" + "instant=" + instant + '}';
    }
  }

  public class CommandWithActorRef implements TestMessage {
    public final String name;
    public final ActorRef replyTo;

    public CommandWithActorRef(String name, ActorRef replyTo) {
      this.name = name;
      this.replyTo = replyTo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CommandWithActorRef that = (CommandWithActorRef) o;

      if (!name.equals(that.name)) return false;
      return replyTo.equals(that.replyTo);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + replyTo.hashCode();
      return result;
    }
  }

  public class CommandWithTypedActorRef implements TestMessage {
    public final String name;
    public final akka.actor.typed.ActorRef<String> replyTo;

    public CommandWithTypedActorRef(String name, akka.actor.typed.ActorRef<String> replyTo) {
      this.name = name;
      this.replyTo = replyTo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CommandWithTypedActorRef that = (CommandWithTypedActorRef) o;

      if (!name.equals(that.name)) return false;
      return replyTo.equals(that.replyTo);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + replyTo.hashCode();
      return result;
    }
  }

  public class CommandWithAddress implements TestMessage {
    public final String name;
    public final Address address;

    public CommandWithAddress(String name, Address address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CommandWithAddress that = (CommandWithAddress) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
    }
  }

  public class Event1 implements TestMessage {

    private final String field1;

    public Event1(String field1) {
      this.field1 = field1;
    }

    public String getField1() {
      return field1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Event1 event1 = (Event1) o;

      return field1 != null ? field1.equals(event1.field1) : event1.field1 == null;
    }

    @Override
    public int hashCode() {
      return field1 != null ? field1.hashCode() : 0;
    }
  }

  public class Event2 implements TestMessage {
    private final String field1V2; // renamed from field1
    private final int field2; // new mandatory field

    public Event2(String field1V2, int field2) {
      this.field1V2 = field1V2;
      this.field2 = field2;
    }

    public String getField1V2() {
      return field1V2;
    }

    public int getField2() {
      return field2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Event2 event2 = (Event2) o;

      if (field2 != event2.field2) return false;
      return field1V2 != null ? field1V2.equals(event2.field1V2) : event2.field1V2 == null;
    }

    @Override
    public int hashCode() {
      int result = field1V2 != null ? field1V2.hashCode() : 0;
      result = 31 * result + field2;
      return result;
    }
  }

  public class Event3 implements TestMessage {
    private final String field1V2; // same as in Event2
    private final int field3; // renamed field (was field2)

    public Event3(String field1V2, int field3) {
      this.field1V2 = field1V2;
      this.field3 = field3;
    }

    public String getField1V2() {
      return field1V2;
    }

    public int getField3() {
      return field3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Event3 event3 = (Event3) o;

      if (field3 != event3.field3) return false;
      return field1V2 != null ? field1V2.equals(event3.field1V2) : event3.field1V2 == null;
    }

    @Override
    public int hashCode() {
      int result = field1V2 != null ? field1V2.hashCode() : 0;
      result = 31 * result + field3;
      return result;
    }
  }

  public class Zoo implements TestMessage {
    public final Animal first;

    public Zoo(Animal first) {
      this.first = first;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Zoo zoo = (Zoo) o;

      return first != null ? first.equals(zoo.first) : zoo.first == null;
    }

    @Override
    public int hashCode() {
      return first != null ? first.hashCode() : 0;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Lion lion = (Lion) o;

      return name != null ? name.equals(lion.name) : lion.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }

  public final class Elephant implements Animal {
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
      return name != null ? name.equals(elephant.name) : elephant.name == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + age;
      return result;
    }
  }
  // not defined in JsonSubTypes
  final class Cockroach implements Animal {
    public final String name;

    public Cockroach(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Cockroach cockroach = (Cockroach) o;

      return name != null ? name.equals(cockroach.name) : cockroach.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }

  // A class with non-public fields
  final class ClassWithVisibility {
    public final String publicField = "1234";
    final String defaultField = "abcd";
    protected final String protectedField = "vwxyz";
    private final String privateField = "ABCD";

    public ClassWithVisibility() {}
  }
}
