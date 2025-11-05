/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi;

import static org.junit.Assert.*;

import akka.japi.pf.FI;
import akka.japi.pf.Match;
import org.junit.Assert;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.MatchError;

public class MatchBuilderTest extends JUnitSuite {

  @Test
  public void shouldPassBasicMatchTest() {
    Match<Object, Double> pf =
        Match.create(
            Match.match(
                    Integer.class,
                    new FI.Apply<Integer, Double>() {
                      @Override
                      public Double apply(Integer integer) {
                        return integer * 10.0;
                      }
                    })
                .match(
                    Number.class,
                    new FI.Apply<Number, Double>() {
                      @Override
                      public Double apply(Number number) {
                        return number.doubleValue() * (-10.0);
                      }
                    }));

    assertTrue(
        "An integer should be multiplied by 10",
        Double.valueOf(47110).equals(pf.match(Integer.valueOf(4711))));
    assertTrue(
        "A double should be multiplied by -10",
        Double.valueOf(-47110).equals(pf.match(Double.valueOf(4711))));

    Assert.assertThrows(
        "A string should throw a MatchError", MatchError.class, () -> pf.match("4711"));
  }

  static class GenericClass<T> {
    T val;

    public GenericClass(T val) {
      this.val = val;
    }
  }

  @Test
  public void shouldHandleMatchOnGenericClass() {
    Match<Object, String> pf =
        Match.create(
            Match.matchUnchecked(
                GenericClass.class,
                new FI.Apply<GenericClass<String>, String>() {
                  @Override
                  public String apply(GenericClass<String> stringGenericClass) {
                    return stringGenericClass.val;
                  }
                }));

    assertEquals(
        "String value should be extract from GenericMessage",
        "A",
        pf.match(new GenericClass<>("A")));
  }

  @Test
  public void shouldHandleMatchWithPredicateOnGenericClass() {
    Match<Object, String> pf =
        Match.create(
            Match.matchUnchecked(
                GenericClass.class,
                new FI.TypedPredicate<GenericClass<String>>() {
                  @Override
                  public boolean defined(GenericClass<String> genericClass) {
                    return !genericClass.val.isEmpty();
                  }
                },
                new FI.Apply<GenericClass<String>, String>() {
                  @Override
                  public String apply(GenericClass<String> stringGenericClass) {
                    return stringGenericClass.val;
                  }
                }));

    Assert.assertThrows(
        "empty GenericMessage should throw match error",
        MatchError.class,
        () -> pf.match(new GenericClass<>("")));
  }
}
