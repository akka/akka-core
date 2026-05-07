/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v2a;

// #structural
public class Address {
  public final String street;
  public final String city;
  public final String zipCode;
  public final String country;

  public Address(String street, String city, String zipCode, String country) {
    this.street = street;
    this.city = city;
    this.zipCode = zipCode;
    this.country = country;
  }
}
// #structural
