/*
 * Copyright (C) 2013-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io.japi;

// #message
public class Message {

  public static class Person {
    private final String first;
    private final String last;

    public Person(String first, String last) {
      this.first = first;
      this.last = last;
    }

    public String getFirst() {
      return first;
    }

    public String getLast() {
      return last;
    }
  }

  private final Person[] persons;
  private final double[] happinessCurve;

  public Message(Person[] persons, double[] happinessCurve) {
    this.persons = persons;
    this.happinessCurve = happinessCurve;
  }

  public Person[] getPersons() {
    return persons;
  }

  public double[] getHappinessCurve() {
    return happinessCurve;
  }
}
// #message
