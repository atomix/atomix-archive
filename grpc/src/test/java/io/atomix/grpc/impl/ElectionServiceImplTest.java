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

import io.atomix.core.Atomix;
import io.atomix.grpc.election.ElectionId;
import io.atomix.grpc.election.ElectionServiceGrpc;
import io.atomix.grpc.election.GetLeadershipRequest;
import io.atomix.grpc.election.RunRequest;
import io.atomix.grpc.election.WithdrawRequest;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * gRPC election service test.
 */
public class ElectionServiceImplTest extends GrpcServiceTest<ElectionServiceGrpc.ElectionServiceBlockingStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new ElectionServiceImpl(atomix);
  }

  @Override
  protected ElectionServiceGrpc.ElectionServiceBlockingStub getStub(Channel channel) {
    return ElectionServiceGrpc.newBlockingStub(channel);
  }

  @Test
  public void testGrpcElection() throws Exception {
    ElectionServiceGrpc.ElectionServiceBlockingStub election1 = getStub(1);
    ElectionServiceGrpc.ElectionServiceBlockingStub election2 = getStub(2);

    ElectionId electionId = ElectionId.newBuilder()
        .setName("test-election")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    try {
      election1.getLeadership(GetLeadershipRequest.newBuilder().build());
      fail();
    } catch (Exception e) {
    }

    try {
      election1.getLeadership(GetLeadershipRequest.newBuilder()
          .setId(ElectionId.newBuilder()
              .setName("foo")
              .build())
          .build());
      fail();
    } catch (Exception e) {
    }

    assertEquals(0, election1.getLeadership(GetLeadershipRequest.newBuilder()
        .setId(electionId)
        .build())
        .getLeadership()
        .getTerm());

    assertEquals("a", election1.run(RunRequest.newBuilder()
        .setId(electionId)
        .setCandidate("a")
        .build())
        .getLeadership()
        .getLeader());

    assertEquals("a", election2.run(RunRequest.newBuilder()
        .setId(electionId)
        .setCandidate("b")
        .build())
        .getLeadership()
        .getLeader());

    election1.withdraw(WithdrawRequest.newBuilder()
        .setId(electionId)
        .setCandidate("a")
        .build());

    assertEquals("b", election2.getLeadership(GetLeadershipRequest.newBuilder()
        .setId(electionId)
        .build())
        .getLeadership()
        .getLeader());
  }
}
