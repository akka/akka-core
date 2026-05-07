/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.Address
import akka.event.LogMarker

/**
 * This is public with the purpose to document the used markers and properties of log events.
 * No guarantee that it will remain binary compatible, but the marker names and properties
 * are considered public API and will not be changed without notice.
 */
object RemoteLogMarker {

  /**
   * Marker "akkaFailureDetectorGrowing" of log event when failure detector heartbeat interval
   * is growing too large.
   *
   * @param remoteAddress The address of the node that the failure detector is monitoring. Included as property "akkaRemoteAddress".
   */
  def failureDetectorGrowing(remoteAddress: String): LogMarker =
    LogMarker("akkaFailureDetectorGrowing", Map(LogMarker.Properties.RemoteAddress -> remoteAddress))

  /**
   * Marker "akkaQuarantine" of log event when a node is quarantined.
   *
   * @param remoteAddress The address of the node that is quarantined. Included as property "akkaRemoteAddress".
   * @param remoteAddressUid The address of the node that is quarantined. Included as property "akkaRemoteAddressUid".
   */
  def quarantine(remoteAddress: Address, remoteAddressUid: Option[Long]): LogMarker =
    LogMarker(
      "akkaQuarantine",
      Map(
        LogMarker.Properties.RemoteAddress -> remoteAddress,
        LogMarker.Properties.RemoteAddressUid -> remoteAddressUid.getOrElse("")))

  /**
   * Marker "akkaConnect" of log event when outbound connection is attempted.
   *
   * @param remoteAddress The address of the connected node. Included as property "akkaRemoteAddress".
   * @param remoteAddressUid The address of the connected node. Included as property "akkaRemoteAddressUid".
   */
  def connect(remoteAddress: Address, remoteAddressUid: Option[Long]): LogMarker =
    LogMarker(
      "akkaConnect",
      Map(
        LogMarker.Properties.RemoteAddress -> remoteAddress,
        LogMarker.Properties.RemoteAddressUid -> remoteAddressUid.getOrElse("")))

  /**
   * Marker "akkaDisconnected" of log event when outbound connection is closed.
   *
   * @param remoteAddress The address of the disconnected node. Included as property "akkaRemoteAddress".
   * @param remoteAddressUid The address of the disconnected node. Included as property "akkaRemoteAddressUid".
   */
  def disconnected(remoteAddress: Address, remoteAddressUid: Option[Long]): LogMarker =
    LogMarker(
      "akkaDisconnected",
      Map(
        LogMarker.Properties.RemoteAddress -> remoteAddress,
        LogMarker.Properties.RemoteAddressUid -> remoteAddressUid.getOrElse("")))
}
