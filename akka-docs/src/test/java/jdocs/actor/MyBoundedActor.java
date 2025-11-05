/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

// #my-bounded-classic-actor
import akka.dispatch.BoundedMessageQueueSemantics;
import akka.dispatch.RequiresMessageQueue;

public class MyBoundedActor extends MyActor
    implements RequiresMessageQueue<BoundedMessageQueueSemantics> {}
// #my-bounded-classic-actor
