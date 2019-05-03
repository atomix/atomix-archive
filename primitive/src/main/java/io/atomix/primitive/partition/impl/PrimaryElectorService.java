/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceOperationRegistry;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.SessionManagedPrimitiveService;
import io.atomix.primitive.session.Session;
import io.atomix.utils.component.Component;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.stream.StreamHandler;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Primary elector service.
 * <p>
 * This state machine orders candidates and assigns primaries based on the distribution of primaries in the cluster
 * such that primaries are evenly distributed across the cluster.
 */
public class PrimaryElectorService extends SessionManagedPrimitiveService {
  public static final Type TYPE = new Type();

  /**
   * Map service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "primary-elector";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new PrimaryElectorService();
    }
  }

  private static final Duration REBALANCE_DURATION = Duration.ofSeconds(15);

  private Map<PartitionId, ElectionState> elections = new HashMap<>();
  private Scheduled rebalanceTimer;

  @Override
  public void backup(OutputStream output) throws IOException {
    PrimaryElectorSnapshot.newBuilder()
        .addAllElections(elections.values().stream()
            .map(election -> io.atomix.primitive.partition.impl.ElectionState.newBuilder()
                .setPartitionId(election.partitionId)
                .setPrimary(ElectionCandidate.newBuilder()
                    .setMember(election.primary.member)
                    .setSessionId(election.primary.sessionId)
                    .build())
                .setTerm(election.term)
                .setTimestamp(election.termStartTime)
                .addAllCandidates(election.registrations.stream()
                    .map(registration -> ElectionCandidate.newBuilder()
                        .setMember(registration.member)
                        .setSessionId(registration.sessionId)
                        .build())
                    .collect(Collectors.toList()))
                .build())
            .collect(Collectors.toList()))
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    PrimaryElectorSnapshot snapshot = PrimaryElectorSnapshot.parseFrom(input);
    elections = new HashMap<>();
    snapshot.getElectionsList().forEach(election ->
        elections.put(election.getPartitionId(), new ElectionState(
            election.getPartitionId(),
            election.getCandidatesList().stream()
                .map(candidate -> new Registration(
                    candidate.getMember(),
                    candidate.getSessionId()))
                .collect(Collectors.toList()),
            new Registration(
                election.getPrimary().getMember(),
                election.getPrimary().getSessionId()),
            election.getTerm(),
            election.getTimestamp())));
    scheduleRebalance();
  }

  @Override
  protected void configure(ServiceOperationRegistry registry) {
    registry.register(
        PrimaryElectorOperations.ENTER,
        this::enter,
        EnterRequest::parseFrom,
        EnterResponse::toByteArray);
    registry.register(
        PrimaryElectorOperations.GET_TERM,
        this::getTerm,
        GetTermRequest::parseFrom,
        GetTermResponse::toByteArray);
    registry.register(
        PrimaryElectorOperations.STREAM_EVENTS,
        PrimaryElectorOperations.EVENT_STREAM,
        this::stream,
        ListenRequest::parseFrom,
        PrimaryElectionEvent::toByteArray);
  }

  private void stream(ListenRequest request, StreamHandler<PrimaryElectionEvent> handler) {
    // Keep the stream open.
  }

  private void notifyTermChange(PartitionId partitionId, PrimaryTerm term) {
    getSessions()
        .forEach(session -> session.getStreams(PrimaryElectorOperations.EVENT_STREAM)
            .forEach(stream -> stream.next(PrimaryElectionEvent.newBuilder()
                .setPartitionId(partitionId)
                .setTerm(term)
                .build())));
  }

  /**
   * Schedules rebalancing of primaries.
   */
  private void scheduleRebalance() {
    if (rebalanceTimer != null) {
      rebalanceTimer.cancel();
    }
    rebalanceTimer = getScheduler().schedule(REBALANCE_DURATION, this::rebalance);
  }

  /**
   * Periodically rebalances primaries.
   */
  private void rebalance() {
    boolean rebalanced = false;
    for (ElectionState election : elections.values()) {
      // Count the total number of primaries for this election's primary.
      int primaryCount = election.countPrimaries(election.primary);

      // Find the registration with the fewest number of primaries.
      int minCandidateCount = 0;
      for (Registration candidate : election.registrations) {
        if (minCandidateCount == 0) {
          minCandidateCount = election.countPrimaries(candidate);
        } else {
          minCandidateCount = Math.min(minCandidateCount, election.countPrimaries(candidate));
        }
      }

      // If the primary count for the current primary is more than that of the candidate with the fewest
      // primaries then transfer leadership to the candidate.
      if (minCandidateCount < primaryCount) {
        for (Registration candidate : election.registrations) {
          if (election.countPrimaries(candidate) < primaryCount) {
            PrimaryTerm oldTerm = election.term();
            elections.put(election.partitionId, election.transfer(candidate.member()));
            PrimaryTerm newTerm = term(election.partitionId);
            if (!Objects.equals(oldTerm, newTerm)) {
              notifyTermChange(election.partitionId, newTerm);
              rebalanced = true;
            }
          }
        }
      }
    }

    // If some elections were rebalanced, reschedule another rebalance after an interval to give the cluster a
    // change to recognize the primary change and replicate state first.
    if (rebalanced) {
      scheduleRebalance();
    }
  }

  /**
   * Applies an {@link EnterRequest} commit.
   *
   * @param request request
   * @return topic leader. If no previous leader existed this is the node that just entered the race.
   */
  protected EnterResponse enter(EnterRequest request) {
    try {
      PartitionId partitionId = request.getPartitionId();
      PrimaryTerm oldTerm = term(partitionId);
      Registration registration = new Registration(
          request.getMember(),
          getCurrentSession().sessionId().id());
      PrimaryTerm newTerm = elections.compute(partitionId, (k, v) -> {
        if (v == null) {
          return new ElectionState(partitionId, registration);
        } else {
          if (!v.isDuplicate(registration)) {
            return new ElectionState(v).addRegistration(registration);
          } else {
            return v;
          }
        }
      })
          .term();

      if (!Objects.equals(oldTerm, newTerm)) {
        notifyTermChange(partitionId, newTerm);
        scheduleRebalance();
      }
      return EnterResponse.newBuilder()
          .setTerm(newTerm)
          .build();
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Applies an {@link GetTermRequest} commit.
   *
   * @param request GetTermRequest request
   * @return leader
   */
  protected GetTermResponse getTerm(GetTermRequest request) {
    PartitionId partitionId = request.getPartitionId();
    try {
      return GetTermResponse.newBuilder()
          .setTerm(term(partitionId))
          .build();
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private PrimaryTerm term(PartitionId partitionId) {
    ElectionState electionState = elections.get(partitionId);
    return electionState != null ? electionState.term() : null;
  }

  private void onSessionEnd(Session session) {
    Set<PartitionId> partitions = elections.keySet();
    partitions.forEach(partitionId -> {
      PrimaryTerm oldTerm = term(partitionId);
      elections.compute(partitionId, (k, v) -> v.cleanup(session));
      PrimaryTerm newTerm = term(partitionId);
      if (!Objects.equals(oldTerm, newTerm)) {
        notifyTermChange(partitionId, newTerm);
        scheduleRebalance();
      }
    });
  }

  private static class Registration {
    private final GroupMember member;
    private final long sessionId;

    Registration(GroupMember member, long sessionId) {
      this.member = member;
      this.sessionId = sessionId;
    }

    public GroupMember member() {
      return member;
    }

    public long sessionId() {
      return sessionId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("member", member)
          .add("session", sessionId)
          .toString();
    }
  }

  private class ElectionState {
    private final PartitionId partitionId;
    private final Registration primary;
    private final long term;
    private final long termStartTime;
    private final List<Registration> registrations;

    ElectionState(
        PartitionId partitionId,
        Registration registration) {
      registrations = Arrays.asList(registration);
      termStartTime = System.currentTimeMillis();
      primary = registration;
      this.partitionId = partitionId;
      this.term = 1;
    }

    ElectionState(ElectionState other) {
      partitionId = other.partitionId;
      registrations = Lists.newArrayList(other.registrations);
      primary = other.primary;
      term = other.term;
      termStartTime = other.termStartTime;
    }

    ElectionState(
        PartitionId partitionId,
        List<Registration> registrations,
        Registration primary,
        long term,
        long termStartTime) {
      this.partitionId = partitionId;
      this.registrations = Lists.newArrayList(registrations);
      this.primary = primary;
      this.term = term;
      this.termStartTime = termStartTime;
    }

    ElectionState cleanup(Session session) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> r.sessionId() != session.sessionId().id())
                .collect(Collectors.toList());
        if (primary.sessionId() == session.sessionId().id()) {
          if (!updatedRegistrations.isEmpty()) {
            return new ElectionState(
                partitionId,
                updatedRegistrations,
                updatedRegistrations.get(0),
                term + 1,
                System.currentTimeMillis());
          } else {
            return new ElectionState(
                partitionId,
                updatedRegistrations,
                null,
                term,
                termStartTime);
          }
        } else {
          return new ElectionState(
              partitionId,
              updatedRegistrations,
              primary,
              term,
              termStartTime);
        }
      } else {
        return this;
      }
    }

    boolean isDuplicate(Registration registration) {
      return registrations.stream()
          .anyMatch(r -> r.sessionId() == registration.sessionId());
    }

    PrimaryTerm term() {
      return PrimaryTerm.newBuilder()
          .setTerm(term)
          .setPrimary(primary())
          .addAllCandidates(candidates())
          .build();
    }

    GroupMember primary() {
      if (primary == null) {
        return null;
      } else {
        return primary.member();
      }
    }

    List<GroupMember> candidates() {
      return registrations.stream().map(registration -> registration.member()).collect(Collectors.toList());
    }

    ElectionState addRegistration(Registration registration) {
      if (!registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId())) {
        List<Registration> updatedRegistrations = new LinkedList<>(registrations);

        boolean added = false;
        int registrationCount = countPrimaries(registration);
        for (int i = 0; i < registrations.size(); i++) {
          if (countPrimaries(registrations.get(i)) > registrationCount) {
            updatedRegistrations.add(i, registration);
            added = true;
            break;
          }
        }

        if (!added) {
          updatedRegistrations.add(registration);
        }

        List<Registration> sortedRegistrations = sortRegistrations(updatedRegistrations);

        Registration firstRegistration = sortedRegistrations.get(0);
        Registration leader = this.primary;
        long term = this.term;
        long termStartTime = this.termStartTime;
        if (leader == null || !leader.equals(firstRegistration)) {
          leader = firstRegistration;
          term = this.term + 1;
          termStartTime = System.currentTimeMillis();
        }
        return new ElectionState(
            partitionId,
            sortedRegistrations,
            leader,
            term,
            termStartTime);
      }
      return this;
    }

    List<Registration> sortRegistrations(List<Registration> registrations) {
      // Count the number of distinct groups in the registrations list.
      int groupCount = (int) registrations.stream()
          .map(r -> r.member().getMemberGroupId())
          .distinct()
          .count();

      Set<MemberGroupId> groups = new HashSet<>();
      List<Registration> sortedRegistrations = new LinkedList<>();

      // Loop until all registrations have been sorted.
      while (!registrations.isEmpty()) {
        // Clear the list of consumed groups.
        groups.clear();

        // For each registration, check if it can be added to the registrations list.
        Iterator<Registration> iterator = registrations.iterator();
        while (iterator.hasNext()) {
          Registration registration = iterator.next();

          // If the registration's group has not been added to the list, add the registration.
          if (groups.add(MemberGroupId.from(registration.member().getMemberGroupId()))) {
            sortedRegistrations.add(registration);
            iterator.remove();

            // If an instance of a registration from each group has been added, reset the list of registrations.
            if (groups.size() == groupCount) {
              groups.clear();
            }
          }
        }
      }
      return sortedRegistrations;
    }

    int countPrimaries(Registration registration) {
      if (registration == null) {
        return 0;
      }

      return (int) elections.entrySet().stream()
          .filter(entry -> !entry.getKey().equals(partitionId))
          .filter(entry -> entry.getValue().primary != null)
          .filter(entry -> {
            // Get the topic leader's identifier and a list of session identifiers.
            // Then return true if the leader's identifier matches any of the session's candidates.
            GroupMember leaderId = entry.getValue().primary();
            List<GroupMember> sessionCandidates = entry.getValue().registrations.stream()
                .filter(r -> r.sessionId == registration.sessionId)
                .map(r -> r.member())
                .collect(Collectors.toList());
            return sessionCandidates.stream()
                .anyMatch(candidate -> Objects.equals(candidate, leaderId));
          })
          .count();
    }

    ElectionState transfer(GroupMember member) {
      Registration newLeader = registrations.stream()
          .filter(r -> Objects.equals(r.member(), member))
          .findFirst()
          .orElse(null);
      if (newLeader != null) {
        return new ElectionState(
            partitionId,
            registrations,
            newLeader,
            term + 1,
            System.currentTimeMillis());
      } else {
        return this;
      }
    }
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
