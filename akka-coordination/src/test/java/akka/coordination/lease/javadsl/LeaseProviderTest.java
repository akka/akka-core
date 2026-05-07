/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease.javadsl;

import static org.junit.Assert.assertEquals;

import akka.actor.ActorSystem;
import akka.coordination.lease.scaladsl.LeaseProviderSpec;
import akka.testkit.AkkaJUnitActorSystemResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class LeaseProviderTest {
  @Rule
  public AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("LoggingAdapterTest", LeaseProviderSpec.config());

  private ActorSystem system = null;

  @Before
  public void before() {
    system = actorSystemResource.getSystem();
  }

  @Test
  public void loadLeaseImpl() {
    Lease leaseA = LeaseProvider.get(system).getLease("a", "lease-a", "owner1");

    assertEquals("a", leaseA.getSettings().leaseName());
    assertEquals("owner1", leaseA.getSettings().ownerName());
    assertEquals("value1", leaseA.getSettings().leaseConfig().getString("key1"));

    Lease leaseB = LeaseProvider.get(system).getLease("b", "lease-b", "owner2");

    assertEquals("b", leaseB.getSettings().leaseName());
    assertEquals("owner2", leaseB.getSettings().ownerName());
    assertEquals("value2", leaseB.getSettings().leaseConfig().getString("key2"));
  }
}
