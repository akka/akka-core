/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class AddressTest extends JUnitSuite {

  @Test
  public void portAddressAccessible() {
    Address address = new Address("akka", "MySystem", "localhost", 2525);
    assertEquals(Optional.of(2525), address.getPort());
    assertEquals(Optional.of("localhost"), address.getHost());
  }
}
