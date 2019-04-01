/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.grpc.impl;

import java.util.stream.Collectors;

import io.atomix.core.Atomix;
import io.atomix.grpc.partitions.GetPartitionGroupsRequest;
import io.atomix.grpc.partitions.GetPartitionGroupsResponse;
import io.atomix.grpc.partitions.PartitionGroup;
import io.atomix.grpc.partitions.PartitionServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * gRPC partition service implementation.
 */
public class PartitionServiceImpl extends PartitionServiceGrpc.PartitionServiceImplBase {
  private final Atomix atomix;

  public PartitionServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  @Override
  public void getPartitionGroups(GetPartitionGroupsRequest request, StreamObserver<GetPartitionGroupsResponse> responseObserver) {
    responseObserver.onNext(GetPartitionGroupsResponse.newBuilder()
        .addAllGroups(atomix.getPartitionService().getPartitionGroups()
            .stream()
            .map(group -> PartitionGroup.newBuilder()
                .setType(group.type().name())
                .setName(group.name())
                .build())
            .collect(Collectors.toList()))
        .build());
    responseObserver.onCompleted();
  }
}
