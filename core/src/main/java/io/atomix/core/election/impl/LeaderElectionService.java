/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.atomix.core.election.Leader;
import io.atomix.core.election.Leadership;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.session.Session;
import io.atomix.utils.component.Component;
import io.atomix.utils.stream.StreamHandler;

/**
 * Leader election service.
 */
public class LeaderElectionService extends AbstractLeaderElectionService {
  public static final Type TYPE = new Type();

  /**
   * Lock service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "election";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new LeaderElectionService();
    }
  }

  private Registration leader;
  private long term;
  private long termStartTime;
  private List<Registration> registrations = new LinkedList<>();
  private AtomicLong termCounter = new AtomicLong();

  @Override
  public EnterResponse enter(EnterRequest request) {
    Leadership<String> oldLeadership = leadership();
    Registration registration = new Registration(request.getId(), getCurrentSession().sessionId().id());
    addRegistration(registration);
    Leadership<String> newLeadership = leadership();

    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setLeader(newLeadership.leader().id())
          .setTerm(newLeadership.leader().term())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
    }
    return EnterResponse.newBuilder()
        .setLeader(newLeadership.leader().id())
        .setTerm(newLeadership.leader().term())
        .setTimestamp(newLeadership.leader().timestamp())
        .addAllCandidates(newLeadership.candidates())
        .build();
  }

  @Override
  public WithdrawResponse withdraw(WithdrawRequest request) {
    Leadership<String> oldLeadership = leadership();
    cleanup(request.getId());
    Leadership<String> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setLeader(newLeadership.leader().id())
          .setTerm(newLeadership.leader().term())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
      return WithdrawResponse.newBuilder()
          .setSucceeded(true)
          .build();
    }
    return WithdrawResponse.newBuilder()
        .setSucceeded(false)
        .build();
  }

  @Override
  public AnointResponse anoint(AnointRequest request) {
    Leadership<String> oldLeadership = leadership();
    Registration newLeader = registrations.stream()
        .filter(r -> r.id().equals(request.getId()))
        .findFirst()
        .orElse(null);
    if (newLeader != null) {
      this.leader = newLeader;
      this.term = termCounter.incrementAndGet();
      this.termStartTime = getCurrentTimestamp();
    }
    Leadership<String> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setLeader(newLeadership.leader().id())
          .setTerm(newLeadership.leader().term())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
    }
    return AnointResponse.newBuilder()
        .setSucceeded(leader != null && leader.id().equals(request.getId()))
        .build();
  }

  @Override
  public PromoteResponse promote(PromoteRequest request) {
    Leadership<String> oldLeadership = leadership();
    boolean containsCandidate = oldLeadership.candidates().stream()
        .anyMatch(a -> a.equals(request.getId()));
    if (!containsCandidate) {
      return PromoteResponse.newBuilder()
          .setSucceeded(false)
          .build();
    }

    Registration registration = registrations.stream()
        .filter(r -> r.id().equals(request.getId()))
        .findFirst()
        .orElse(null);
    List<Registration> updatedRegistrations = new ArrayList<>();
    updatedRegistrations.add(registration);
    registrations.stream()
        .filter(r -> !r.id().equals(request.getId()))
        .forEach(updatedRegistrations::add);
    this.registrations = updatedRegistrations;
    Leadership<String> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setLeader(newLeadership.leader().id())
          .setTerm(newLeadership.leader().term())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
    }
    return PromoteResponse.newBuilder()
        .setSucceeded(true)
        .build();
  }

  @Override
  public EvictResponse evict(EvictRequest request) {
    Leadership<String> oldLeadership = leadership();
    Optional<Registration> registration =
        registrations.stream().filter(r -> r.id().equals(request.getId())).findFirst();
    if (registration.isPresent()) {
      List<Registration> updatedRegistrations =
          registrations.stream()
              .filter(r -> !r.id().equals(request.getId()))
              .collect(Collectors.toList());
      if (leader.id().equals(request.getId())) {
        if (!updatedRegistrations.isEmpty()) {
          this.registrations = updatedRegistrations;
          this.leader = updatedRegistrations.get(0);
          this.term = termCounter.incrementAndGet();
          this.termStartTime = getCurrentTimestamp();
        } else {
          this.registrations = updatedRegistrations;
          this.leader = null;
        }
      } else {
        this.registrations = updatedRegistrations;
      }
    }
    Leadership<String> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setLeader(newLeadership.leader().id())
          .setTerm(newLeadership.leader().term())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
      return EvictResponse.newBuilder()
          .setSucceeded(true)
          .build();
    }
    return EvictResponse.newBuilder()
        .setSucceeded(false)
        .build();
  }

  @Override
  public GetLeadershipResponse getLeadership(GetLeadershipRequest request) {
    Leadership<String> leadership = leadership();
    return GetLeadershipResponse.newBuilder()
        .setLeader(leadership.leader().id())
        .setTerm(leadership.leader().term())
        .setTimestamp(leadership.leader().timestamp())
        .addAllCandidates(leadership.candidates())
        .build();
  }

  @Override
  public void listen(ListenRequest request, StreamHandler<ListenResponse> handler) {
    // Keep the stream open.
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    // Close the stream.
    StreamHandler<ListenRequest> stream = getCurrentSession().getStream(request.getStreamId());
    if (stream != null) {
      stream.complete();
    }
    return UnlistenResponse.newBuilder().build();
  }

  /**
   * Publishes an election event to all registered sessions.
   *
   * @param event the event to publish
   */
  protected void onEvent(ListenResponse event) {
    getSessions()
        .forEach(session -> session.getStreams(LeaderElectionOperations.LISTEN_STREAM)
            .forEach(stream -> stream.next(event)));
  }

  private Leadership<String> leadership() {
    return new Leadership<>(leader(), candidates());
  }

  private void onSessionEnd(Session session) {
    Leadership<String> oldLeadership = leadership();
    cleanup(session);
    Leadership<String> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      onEvent(ListenResponse.newBuilder()
          .setTerm(newLeadership.leader().term())
          .setLeader(newLeadership.leader().id())
          .setTimestamp(newLeadership.leader().timestamp())
          .addAllCandidates(newLeadership.candidates())
          .build());
    }
  }

  private static class Registration {
    private final String id;
    private final long sessionId;

    protected Registration(String id, long sessionId) {
      this.id = id;
      this.sessionId = sessionId;
    }

    protected String id() {
      return id;
    }

    protected long sessionId() {
      return sessionId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", id)
          .add("sessionId", sessionId)
          .toString();
    }
  }

  protected void cleanup(String id) {
    Optional<Registration> registration =
        registrations.stream().filter(r -> r.id().equals(id)).findFirst();
    if (registration.isPresent()) {
      List<Registration> updatedRegistrations =
          registrations.stream()
              .filter(r -> !r.id().equals(id))
              .collect(Collectors.toList());
      if (leader.id().equals(id)) {
        if (!updatedRegistrations.isEmpty()) {
          this.registrations = updatedRegistrations;
          this.leader = updatedRegistrations.get(0);
          this.term = termCounter.incrementAndGet();
          this.termStartTime = getCurrentTimestamp();
        } else {
          this.registrations = updatedRegistrations;
          this.leader = null;
        }
      } else {
        this.registrations = updatedRegistrations;
      }
    }
  }

  protected void cleanup(Session session) {
    Optional<Registration> registration =
        registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
    if (registration.isPresent()) {
      List<Registration> updatedRegistrations =
          registrations.stream()
              .filter(r -> r.sessionId() != session.sessionId().id())
              .collect(Collectors.toList());
      if (leader.sessionId() == session.sessionId().id()) {
        if (!updatedRegistrations.isEmpty()) {
          this.registrations = updatedRegistrations;
          this.leader = updatedRegistrations.get(0);
          this.term = termCounter.incrementAndGet();
          this.termStartTime = getCurrentTimestamp();
        } else {
          this.registrations = updatedRegistrations;
          this.leader = null;
        }
      } else {
        this.registrations = updatedRegistrations;
      }
    }
  }

  protected Leader<String> leader() {
    if (leader == null) {
      return null;
    } else {
      String leaderId = leader.id();
      return new Leader<>(leaderId, term, termStartTime);
    }
  }

  protected List<String> candidates() {
    return registrations.stream().map(registration -> registration.id()).collect(Collectors.toList());
  }

  protected void addRegistration(Registration registration) {
    if (registrations.stream().noneMatch(r -> registration.id.equals(r.id()))) {
      List<Registration> updatedRegistrations = new LinkedList<>(registrations);
      updatedRegistrations.add(registration);
      boolean newLeader = leader == null;
      this.registrations = updatedRegistrations;
      if (newLeader) {
        this.leader = registration;
        this.term = termCounter.incrementAndGet();
        this.termStartTime = getCurrentTimestamp();
      }
    }
  }

  @Override
  protected void backup(OutputStream output) throws IOException {
    LeaderElectionSnapshot.Builder builder = LeaderElectionSnapshot.newBuilder()
        .setTerm(term)
        .setTimestamp(termStartTime)
        .addAllCandidates(registrations.stream()
            .map(registration -> LeaderElectionRegistration.newBuilder()
                .setId(registration.id)
                .setSessionId(registration.sessionId)
                .build())
            .collect(Collectors.toList()));
    if (leader != null) {
      builder.setLeader(LeaderElectionRegistration.newBuilder()
          .setId(leader.id)
          .setSessionId(leader.sessionId)
          .build());
    }
    builder.build().writeTo(output);
  }

  @Override
  protected void restore(InputStream input) throws IOException {
    LeaderElectionSnapshot snapshot = LeaderElectionSnapshot.parseFrom(input);
    term = snapshot.getTerm();
    termStartTime = snapshot.getTimestamp();
    if (snapshot.hasLeader()) {
      leader = new Registration(snapshot.getLeader().getId(), snapshot.getLeader().getSessionId());
    } else {
      leader = null;
    }
    registrations = new LinkedList<>(snapshot.getCandidatesList().stream()
        .map(registration -> new Registration(registration.getId(), registration.getSessionId()))
        .collect(Collectors.toList()));
  }

  @Override
  public void onExpire(Session session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(Session session) {
    onSessionEnd(session);
  }
}