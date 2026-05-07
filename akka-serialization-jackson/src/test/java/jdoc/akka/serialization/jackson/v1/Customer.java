/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.serialization.jackson.v1;

import akka.serialization.jackson.JsonSerializable;

// #structural
public class Customer implements JsonSerializable {
  public final String name;
  public final String street;
  public final String city;
  public final String zipCode;
  public final String country;

  public Customer(String name, String street, String city, String zipCode, String country) {
    this.name = name;
    this.street = street;
    this.city = city;
    this.zipCode = zipCode;
    this.country = country;
  }
}
// #structural
