/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.actor.Address
import akka.util.Version

object TestMember {
  def apply(address: Address, status: MemberStatus): Member =
    apply(address, status, Set.empty[String])

  def apply(address: Address, status: MemberStatus, upNumber: Int, dc: ClusterSettings.DataCenter): Member =
    apply(address, status, Set.empty, dc, upNumber)

  def apply(
      address: Address,
      status: MemberStatus,
      roles: Set[String],
      dataCenter: ClusterSettings.DataCenter = ClusterSettings.DefaultDataCenter,
      upNumber: Int = Int.MaxValue,
      appVersion: Version = Version.Zero): Member =
    withUniqueAddress(UniqueAddress(address, 0L), status, roles, dataCenter, upNumber, appVersion)

  def withUniqueAddress(
      uniqueAddress: UniqueAddress,
      status: MemberStatus,
      roles: Set[String],
      dataCenter: ClusterSettings.DataCenter,
      upNumber: Int = Int.MaxValue,
      appVersion: Version = Version.Zero): Member =
    new Member(uniqueAddress, upNumber, status, roles + (ClusterSettings.DcRolePrefix + dataCenter), appVersion)
}
