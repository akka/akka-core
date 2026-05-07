/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence.testkit;

import akka.persistence.testkit.DeleteEvents;
import akka.persistence.testkit.DeleteSnapshotByMeta;
import akka.persistence.testkit.DeleteSnapshotsByCriteria;
import akka.persistence.testkit.JournalOperation;
import akka.persistence.testkit.ProcessingPolicy;
import akka.persistence.testkit.ProcessingResult;
import akka.persistence.testkit.ProcessingSuccess;
import akka.persistence.testkit.ReadEvents;
import akka.persistence.testkit.ReadSeqNum;
import akka.persistence.testkit.ReadSnapshot;
import akka.persistence.testkit.Reject;
import akka.persistence.testkit.SnapshotOperation;
import akka.persistence.testkit.StorageFailure;
import akka.persistence.testkit.WriteEvents;
import akka.persistence.testkit.WriteSnapshot;

public class TestKitExamples {

  // #set-event-storage-policy
  class SampleEventStoragePolicy implements ProcessingPolicy<JournalOperation> {

    // you can use internal state, it does not need to be thread safe
    int count = 1;

    @Override
    public ProcessingResult tryProcess(String processId, JournalOperation processingUnit) {
      // check the type of operation and react with success or with reject or with failure.
      // if you return ProcessingSuccess the operation will be performed, otherwise not.
      if (count < 10) {
        count += 1;
        if (processingUnit instanceof ReadEvents) {
          ReadEvents read = (ReadEvents) processingUnit;
          if (read.batch().nonEmpty()) {
            ProcessingSuccess.getInstance();
          } else {
            return StorageFailure.create();
          }
        } else if (processingUnit instanceof WriteEvents) {
          return ProcessingSuccess.getInstance();
        } else if (processingUnit instanceof DeleteEvents) {
          return ProcessingSuccess.getInstance();
        } else if (processingUnit.equals(ReadSeqNum.getInstance())) {
          return Reject.create();
        }
        // you can set your own exception
        return StorageFailure.create(new RuntimeException("your exception"));
      } else {
        return ProcessingSuccess.getInstance();
      }
    }
  }
  // #set-event-storage-policy

  // #set-snapshot-storage-policy
  class SnapshotStoragePolicy implements ProcessingPolicy<SnapshotOperation> {

    // you can use internal state, it doesn't need to be thread safe
    int count = 1;

    @Override
    public ProcessingResult tryProcess(String processId, SnapshotOperation processingUnit) {
      // check the type of operation and react with success or with failure.
      // if you return ProcessingSuccess the operation will be performed, otherwise not.
      if (count < 10) {
        count += 1;
        if (processingUnit instanceof ReadSnapshot) {
          ReadSnapshot read = (ReadSnapshot) processingUnit;
          if (read.getSnapshot().isPresent()) {
            ProcessingSuccess.getInstance();
          } else {
            return StorageFailure.create();
          }
        } else if (processingUnit instanceof WriteSnapshot) {
          return ProcessingSuccess.getInstance();
        } else if (processingUnit instanceof DeleteSnapshotsByCriteria) {
          return ProcessingSuccess.getInstance();
        } else if (processingUnit instanceof DeleteSnapshotByMeta) {
          return ProcessingSuccess.getInstance();
        }
        // you can set your own exception
        return StorageFailure.create(new RuntimeException("your exception"));
      } else {
        return ProcessingSuccess.getInstance();
      }
    }
  }
  // #set-snapshot-storage-policy

}
