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

import com.google.protobuf.Duration;
import io.atomix.core.Atomix;
import io.atomix.grpc.election.CreateRequest;
import io.atomix.grpc.election.CreateResponse;
import io.atomix.grpc.election.ElectionId;
import io.atomix.grpc.election.EnterRequest;
import io.atomix.grpc.election.EnterResponse;
import io.atomix.grpc.election.LeaderElectionServiceGrpc;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * gRPC leader election service test.
 */
public class LeaderElectionServiceImplTest extends GrpcServiceTest<LeaderElectionServiceGrpc.LeaderElectionServiceFutureStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new LeaderElectionServiceImpl(atomix);
  }

  @Override
  protected LeaderElectionServiceGrpc.LeaderElectionServiceFutureStub getStub(Channel channel) {
    return LeaderElectionServiceGrpc.newFutureStub(channel);
  }

  @Test
  public void testGrpcElection() throws Exception {
    LeaderElectionServiceGrpc.LeaderElectionServiceFutureStub election1 = getStub(1);
    LeaderElectionServiceGrpc.LeaderElectionServiceFutureStub election2 = getStub(2);

    ElectionId electionId = ElectionId.newBuilder()
        .setName("test-election")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    CreateResponse session1 = election1.create(CreateRequest.newBuilder()
        .setId(electionId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(5)
            .build())
        .build())
        .get();

    CreateResponse session2 = election2.create(CreateRequest.newBuilder()
        .setId(electionId)
        .setTimeout(Duration.newBuilder()
            .setSeconds(5)
            .build())
        .build())
        .get();

    EnterResponse enterResponse = election1.enter(EnterRequest.newBuilder()
        .setId(electionId)
        .setHeader(SessionCommandHeader.newBuilder()
            .setPartitionId(session1.getHeader().getPartitionId())
            .setSessionId(session1.getHeader().getSessionId())
            .build())
        .setCandidateId("foo")
        .build())
        .get();
    assertEquals("foo", enterResponse.getLeader());
  }
}
