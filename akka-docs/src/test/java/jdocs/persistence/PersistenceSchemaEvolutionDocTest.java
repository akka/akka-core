/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

import akka.persistence.journal.EventAdapter;
import akka.persistence.journal.EventSeq;
import akka.protobufv3.internal.InvalidProtocolBufferException;
import akka.serialization.SerializerWithStringManifest;
import docs.persistence.ExampleJsonMarshaller;
import docs.persistence.proto.FlightAppModels;
import java.io.NotSerializableException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import spray.json.JsObject;

public class PersistenceSchemaEvolutionDocTest {

  public
  // #protobuf-read-optional-model
  static enum SeatType {
    Window("W"),
    Aisle("A"),
    Other("O"),
    Unknown("");

    private final String code;

    private SeatType(String code) {
      this.code = code;
    }

    public static SeatType fromCode(String c) {
      if (Window.code.equals(c)) return Window;
      else if (Aisle.code.equals(c)) return Aisle;
      else if (Other.code.equals(c)) return Other;
      else return Unknown;
    }
  }
  // #protobuf-read-optional-model

  public
  // #protobuf-read-optional-model
  static class SeatReserved {
    public final String letter;
    public final int row;
    public final SeatType seatType;

    public SeatReserved(String letter, int row, SeatType seatType) {
      this.letter = letter;
      this.row = row;
      this.seatType = seatType;
    }
  }
  // #protobuf-read-optional-model

  public
  // #protobuf-read-optional
  /**
   * Example serializer impl which uses protocol buffers generated classes (proto.*) to perform the
   * to/from binary marshalling.
   */
  static class AddedFieldsSerializerWithProtobuf extends SerializerWithStringManifest {
    @Override
    public int identifier() {
      return 67876;
    }

    private final String seatReservedManifest = SeatReserved.class.getName();

    @Override
    public String manifest(Object o) {
      return o.getClass().getName();
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
      if (seatReservedManifest.equals(manifest)) {
        // use generated protobuf serializer
        try {
          return seatReserved(FlightAppModels.SeatReserved.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalArgumentException(e.getMessage());
        }
      } else {
        throw new NotSerializableException("Unable to handle manifest: " + manifest);
      }
    }

    @Override
    public byte[] toBinary(Object o) {
      if (o instanceof SeatReserved) {
        SeatReserved s = (SeatReserved) o;
        return FlightAppModels.SeatReserved.newBuilder()
            .setRow(s.row)
            .setLetter(s.letter)
            .setSeatType(s.seatType.code)
            .build()
            .toByteArray();

      } else {
        throw new IllegalArgumentException("Unable to handle: " + o);
      }
    }

    // -- fromBinary helpers --

    private SeatReserved seatReserved(FlightAppModels.SeatReserved p) {
      return new SeatReserved(p.getLetter(), p.getRow(), seatType(p));
    }

    // handle missing field by assigning "Unknown" value
    private SeatType seatType(FlightAppModels.SeatReserved p) {
      if (p.hasSeatType()) return SeatType.fromCode(p.getSeatType());
      else return SeatType.Unknown;
    }
  }
  // #protobuf-read-optional

  public static class RenamePlainJson {
    public
    // #rename-plain-json
    static class JsonRenamedFieldAdapter implements EventAdapter {
      // use your favorite json library
      private final ExampleJsonMarshaller marshaller = new ExampleJsonMarshaller();

      private final String V1 = "v1";
      private final String V2 = "v2";

      // this could be done independently for each event type
      @Override
      public String manifest(Object event) {
        return V2;
      }

      @Override
      public JsObject toJournal(Object event) {
        return marshaller.toJson(event);
      }

      @Override
      public EventSeq fromJournal(Object event, String manifest) {
        if (event instanceof JsObject) {
          JsObject json = (JsObject) event;
          if (V1.equals(manifest)) json = rename(json, "code", "seatNr");
          return EventSeq.single(json);
        } else {
          throw new IllegalArgumentException(
              "Can only work with JSON, was: " + event.getClass().getName());
        }
      }

      private JsObject rename(JsObject json, String from, String to) {
        // use your favorite json library to rename the field
        JsObject renamed = json;
        return renamed;
      }
    }
    // #rename-plain-json
  }

  public static class SimplestCustomSerializer {

    public
    // #simplest-custom-serializer-model
    static class Person {
      public final String name;
      public final String surname;

      public Person(String name, String surname) {
        this.name = name;
        this.surname = surname;
      }
    }
    // #simplest-custom-serializer-model

    public
    // #simplest-custom-serializer
    /**
     * Simplest possible serializer, uses a string representation of the Person class.
     *
     * <p>Usually a serializer like this would use a library like: protobuf, kryo, avro, cap'n
     * proto, flatbuffers, SBE or some other dedicated serializer backend to perform the actual
     * to/from bytes marshalling.
     */
    static class SimplestPossiblePersonSerializer extends SerializerWithStringManifest {
      private final Charset utf8 = StandardCharsets.UTF_8;

      private final String personManifest = Person.class.getName();

      // unique identifier of the serializer
      @Override
      public int identifier() {
        return 1234567;
      }

      // extract manifest to be stored together with serialized object
      @Override
      public String manifest(Object o) {
        return o.getClass().getName();
      }

      // serialize the object
      @Override
      public byte[] toBinary(Object obj) {
        if (obj instanceof Person) {
          Person p = (Person) obj;
          return (p.name + "|" + p.surname).getBytes(utf8);
        } else {
          throw new IllegalArgumentException(
              "Unable to serialize to bytes, clazz was: " + obj.getClass().getName());
        }
      }

      // deserialize the object, using the manifest to indicate which logic to apply
      @Override
      public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
        if (personManifest.equals(manifest)) {
          String nameAndSurname = new String(bytes, utf8);
          String[] parts = nameAndSurname.split("[|]");
          return new Person(parts[0], parts[1]);
        } else {
          throw new NotSerializableException(
              "Unable to deserialize from bytes, manifest was: "
                  + manifest
                  + "! Bytes length: "
                  + bytes.length);
        }
      }
    }
    // #simplest-custom-serializer
  }

  public static class SamplePayload {
    private final Object payload;

    public SamplePayload(Object payload) {
      this.payload = payload;
    }

    public Object getPayload() {
      return payload;
    }
  }

  // #split-events-during-recovery
  interface Version1 {};

  interface Version2 {}

  // #split-events-during-recovery
  public
  // #split-events-during-recovery
  // V1 event:
  static class UserDetailsChanged implements Version1 {
    public final String name;
    public final String address;

    public UserDetailsChanged(String name, String address) {
      this.name = name;
      this.address = address;
    }
  }

  // #split-events-during-recovery
  public
  // #split-events-during-recovery
  // corresponding V2 events:
  static class UserNameChanged implements Version2 {
    public final String name;

    public UserNameChanged(String name) {
      this.name = name;
    }
  }
  // #split-events-during-recovery
  public
  // #split-events-during-recovery
  static class UserAddressChanged implements Version2 {
    public final String address;

    public UserAddressChanged(String address) {
      this.address = address;
    }
  }

  // #split-events-during-recovery
  public
  // #split-events-during-recovery
  // event splitting adapter:
  static class UserEventsAdapter implements EventAdapter {
    @Override
    public String manifest(Object event) {
      return "";
    }

    @Override
    public EventSeq fromJournal(Object event, String manifest) {
      if (event instanceof UserDetailsChanged) {
        UserDetailsChanged c = (UserDetailsChanged) event;
        if (c.name == null) return EventSeq.single(new UserAddressChanged(c.address));
        else if (c.address == null) return EventSeq.single(new UserNameChanged(c.name));
        else return EventSeq.create(new UserNameChanged(c.name), new UserAddressChanged(c.address));
      } else {
        return EventSeq.single(event);
      }
    }

    @Override
    public Object toJournal(Object event) {
      return event;
    }
  }
  // #split-events-during-recovery

  public static class CustomerBlinked {
    public final long customerId;

    public CustomerBlinked(long customerId) {
      this.customerId = customerId;
    }
  }

  public
  // #string-serializer-skip-deleved-event-by-manifest
  static class EventDeserializationSkipped {
    public static EventDeserializationSkipped instance = new EventDeserializationSkipped();

    private EventDeserializationSkipped() {}
  }

  // #string-serializer-skip-deleved-event-by-manifest
  public
  // #string-serializer-skip-deleved-event-by-manifest
  static class RemovedEventsAwareSerializer extends SerializerWithStringManifest {
    private final Charset utf8 = StandardCharsets.UTF_8;
    private final String customerBlinkedManifest = "blinked";

    // unique identifier of the serializer
    @Override
    public int identifier() {
      return 8337;
    }

    // extract manifest to be stored together with serialized object
    @Override
    public String manifest(Object o) {
      if (o instanceof CustomerBlinked) return customerBlinkedManifest;
      else return o.getClass().getName();
    }

    @Override
    public byte[] toBinary(Object o) {
      return o.toString().getBytes(utf8); // example serialization
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) {
      if (customerBlinkedManifest.equals(manifest)) return EventDeserializationSkipped.instance;
      else return new String(bytes, utf8);
    }
  }
  // #string-serializer-skip-deleved-event-by-manifest

  public
  // #string-serializer-skip-deleved-event-by-manifest-adapter
  static class SkippedEventsAwareAdapter implements EventAdapter {
    @Override
    public String manifest(Object event) {
      return "";
    }

    @Override
    public Object toJournal(Object event) {
      return event;
    }

    @Override
    public EventSeq fromJournal(Object event, String manifest) {
      if (event == EventDeserializationSkipped.instance) return EventSeq.empty();
      else return EventSeq.single(event);
    }
  }
  // #string-serializer-skip-deleved-event-by-manifest-adapter

  // #string-serializer-handle-rename
  public
  // #string-serializer-handle-rename
  static class RenamedEventAwareSerializer extends SerializerWithStringManifest {
    private final Charset utf8 = StandardCharsets.UTF_8;

    // unique identifier of the serializer
    @Override
    public int identifier() {
      return 8337;
    }

    private final String oldPayloadClassName =
        "docs.persistence.OldPayload"; // class NOT available anymore
    private final String myPayloadClassName = SamplePayload.class.getName();

    // extract manifest to be stored together with serialized object
    @Override
    public String manifest(Object o) {
      return o.getClass().getName();
    }

    @Override
    public byte[] toBinary(Object o) {
      if (o instanceof SamplePayload) {
        SamplePayload s = (SamplePayload) o;
        return s.payload.toString().getBytes(utf8);
      } else {
        // previously also handled "old" events here.
        throw new IllegalArgumentException(
            "Unable to serialize to bytes, clazz was: " + o.getClass().getName());
      }
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
      if (oldPayloadClassName.equals(manifest)) return new SamplePayload(new String(bytes, utf8));
      else if (myPayloadClassName.equals(manifest))
        return new SamplePayload(new String(bytes, utf8));
      else throw new NotSerializableException("unexpected manifest [" + manifest + "]");
    }
  }
  // #string-serializer-handle-rename

  public
  // #detach-models
  // Domain model - highly optimised for domain language and maybe "fluent" usage
  static class Customer {
    public final String name;

    public Customer(String name) {
      this.name = name;
    }
  }

  // #detach-models
  public
  // #detach-models
  static class Seat {
    public final String code;

    public Seat(String code) {
      this.code = code;
    }

    public SeatBooked bookFor(Customer customer) {
      return new SeatBooked(code, customer);
    }
  }

  // #detach-models
  public
  // #detach-models
  static class SeatBooked {
    public final String code;
    public final Customer customer;

    public SeatBooked(String code, Customer customer) {
      this.code = code;
      this.customer = customer;
    }
  }

  // #detach-models
  public
  // #detach-models
  // Data model - highly optimised for schema evolution and persistence
  static class SeatBookedData {
    public final String code;
    public final String customerName;

    public SeatBookedData(String code, String customerName) {
      this.code = code;
      this.customerName = customerName;
    }
  }
  // #detach-models

  // #detach-models-adapter
  class DetachedModelsAdapter implements EventAdapter {
    @Override
    public String manifest(Object event) {
      return "";
    }

    @Override
    public Object toJournal(Object event) {
      if (event instanceof SeatBooked) {
        SeatBooked s = (SeatBooked) event;
        return new SeatBookedData(s.code, s.customer.name);
      } else {
        throw new IllegalArgumentException("Unsupported: " + event.getClass());
      }
    }

    @Override
    public EventSeq fromJournal(Object event, String manifest) {
      if (event instanceof SeatBookedData) {
        SeatBookedData d = (SeatBookedData) event;
        return EventSeq.single(new SeatBooked(d.code, new Customer(d.customerName)));
      } else {
        throw new IllegalArgumentException("Unsupported: " + event.getClass());
      }
    }
  }
  // #detach-models-adapter

  public
  // #detach-models-adapter-json
  static class JsonDataModelAdapter implements EventAdapter {

    // use your favorite json library
    private final ExampleJsonMarshaller marshaller = new ExampleJsonMarshaller();

    @Override
    public String manifest(Object event) {
      return "";
    }

    @Override
    public JsObject toJournal(Object event) {
      return marshaller.toJson(event);
    }

    @Override
    public EventSeq fromJournal(Object event, String manifest) {
      if (event instanceof JsObject) {
        JsObject json = (JsObject) event;
        return EventSeq.single(marshaller.fromJson(json));
      } else {
        throw new IllegalArgumentException(
            "Unable to fromJournal a non-JSON object! Was: " + event.getClass());
      }
    }
  }
  // #detach-models-adapter-json

}
