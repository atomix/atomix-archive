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

import java.util.Collection;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.core.Atomix;
import io.atomix.grpc.cluster.ClusterServiceGrpc;
import io.atomix.grpc.cluster.GetMemberRequest;
import io.atomix.grpc.cluster.GetMemberResponse;
import io.atomix.grpc.cluster.GetMembersRequest;
import io.atomix.grpc.cluster.GetMembersResponse;
import io.atomix.grpc.cluster.ListenRequest;
import io.atomix.grpc.cluster.Member;
import io.atomix.grpc.cluster.MemberEvent;
import io.atomix.utils.net.Address;
import io.grpc.stub.StreamObserver;

/**
 * gRPC cluster service implementation.
 */
public class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {
  private final Atomix atomix;

  public ClusterServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private Member toMember(io.atomix.cluster.Member member) {
    return member == null ? null : Member.newBuilder()
        .setId(member.id().id())
        .setHost(member.address().host())
        .setPort(member.address().port())
        .setZoneId(member.zone())
        .setRackId(member.rack())
        .setHostId(member.host())
        .build();
  }

  @Override
  public void getMembers(GetMembersRequest request, StreamObserver<GetMembersResponse> responseObserver) {
    Collection<io.atomix.cluster.Member> members = atomix.getMembershipService().getMembers();
    responseObserver.onNext(GetMembersResponse.newBuilder()
        .addAllMembers(members.stream()
            .map(this::toMember)
            .collect(Collectors.toSet()))
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getMember(GetMemberRequest request, StreamObserver<GetMemberResponse> responseObserver) {
    io.atomix.cluster.Member member;
    if (request.getId() != null) {
      member = atomix.getMembershipService().getMember(request.getId());
    } else if (request.getHost() != null) {
      member = atomix.getMembershipService().getMember(Address.from(request.getHost(), request.getPort()));
    } else {
      member = atomix.getMembershipService().getLocalMember();
    }
    responseObserver.onNext(GetMemberResponse.newBuilder()
        .setMember(toMember(member))
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<ListenRequest> listen(StreamObserver<MemberEvent> responseObserver) {
    ClusterMembershipEventListener listener = e -> {
      MemberEvent event = toEvent(e);
      if (event != null) {
        responseObserver.onNext(event);
      }
    };
    atomix.getMembershipService().addListener(listener);

    return new StreamObserver<ListenRequest>() {
      @Override
      public void onNext(ListenRequest value) {
      }

      @Override
      public void onError(Throwable t) {
        atomix.getMembershipService().removeListener(listener);
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        atomix.getMembershipService().removeListener(listener);
        responseObserver.onCompleted();
      }
    };
  }

  private MemberEvent toEvent(ClusterMembershipEvent event) {
    switch (event.type()) {
      case MEMBER_ADDED:
        return MemberEvent.newBuilder()
            .setType(MemberEvent.Type.ADDED)
            .setMember(toMember(event.subject()))
            .build();
      case MEMBER_REMOVED:
        return MemberEvent.newBuilder()
            .setType(MemberEvent.Type.REMOVED)
            .setMember(toMember(event.subject()))
            .build();
      default:
        return null;
    }
  }
}
