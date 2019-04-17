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
package io.atomix.primitive.partition.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimaryElectorServiceTest {
  static long sessionNum = 0;

  @Test
  public void testEnterSinglePartition() {
    PartitionId partition = PartitionId.newBuilder()
        .setGroup("test")
        .setPartition(1)
        .build();
    PrimaryElectorService elector = newService();
    PrimaryTerm term;

    // 1st member to enter should be primary.
    GroupMember m1 = createGroupMember("node1", "group1");
    Session<?> s1 = createSession(m1);
    term = elector.enter(createEnterOp(partition, m1, s1)).getTerm();
    assertEquals(1L, term.getTerm());
    assertEquals(m1, term.getPrimary());
    assertEquals(1, term.getCandidatesList().size());

    // 2nd member to enter should be added to candidates.
    GroupMember m2 = createGroupMember("node2", "group1");
    Session<?> s2 = createSession(m2);
    term = elector.enter(createEnterOp(partition, m2, s2)).getTerm();
    assertEquals(1L, term.getTerm());
    assertEquals(m1, term.getPrimary());
    assertEquals(2, term.getCandidatesList().size());
    assertEquals(m2, term.getCandidatesList().get(1));
  }

  @Test
  public void testEnterSeveralPartitions() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;
    int numParts = 10;
    int numMembers = 20;

    List<List<GroupMember>> allMembers = new ArrayList<>();
    List<PrimaryTerm> terms = new ArrayList<>();
    for (int p = 0; p < numParts; p++) {
      PartitionId partId = PartitionId.newBuilder()
          .setGroup("test")
          .setPartition(p)
          .build();
      allMembers.add(new ArrayList<>());

      // Add all members in same group.
      for (int i = 0; i < numMembers; i++) {
        GroupMember m = createGroupMember("node" + i, "group1");
        allMembers.get(p).add(m);
        Session<?> s = createSession(m);
        term = elector.enter(createEnterOp(partId, m, s)).getTerm();
      }

      if (term != null) {
        terms.add(term);
      }
    }

    // Check primary and candidates in each partition.
    for (int p = 0; p < numParts; p++) {
      assertEquals(1L, terms.get(p).getTerm());
      assertEquals(allMembers.get(p).get(0), terms.get(p).getPrimary());
      assertEquals(numMembers, terms.get(p).getCandidatesList().size());
      for (int i = 0; i < numMembers; i++) {
        assertEquals(allMembers.get(p).get(i), terms.get(p).getCandidatesList().get(i));
      }
    }
  }

  @Test
  public void testEnterSinglePartitionWithGroups() {
    PrimaryElectorService elector = newService();
    PartitionId partId = PartitionId.newBuilder()
        .setGroup("test")
        .setPartition(1)
        .build();
    PrimaryTerm term = null;
    int numMembers = 9;

    // Add 9 members in 3 different groups.
    List<GroupMember> members = new ArrayList<>();
    for (int i = 0; i < numMembers; i++) {
      GroupMember m = createGroupMember("node" + i, "group" + (i / 3));
      members.add(m);
      Session<?> s = createSession(m);
      term = elector.enter(createEnterOp(partId, m, s)).getTerm();
    }

    // Check primary and candidates.
    assertEquals(1L, term.getTerm());
    assertEquals(members.get(0), term.getPrimary());
    assertEquals(numMembers, term.getCandidatesList().size());

    // Check backups are selected in different groups.
    assertEquals(members.get(3), term.getCandidatesList().get(1));
    assertEquals(members.get(6), term.getCandidatesList().get(2));

    assertEquals(members.get(3), term.getCandidatesList().get(1));
    assertEquals(members.get(6), term.getCandidatesList().get(2));
    assertEquals(members.get(1), term.getCandidatesList().get(3));
  }

  @Test
  public void testEnterAndExpireSessions() {
    PrimaryElectorService elector = newService();
    PartitionId partId = PartitionId.newBuilder()
        .setGroup("test")
        .setPartition(1)
        .build();
    PrimaryTerm term = null;
    int numMembers = 9;

    // Add 9 members in 3 different groups.
    List<Session<?>> sessions = new ArrayList<>();
    List<GroupMember> members = new ArrayList<>();
    for (int i = 0; i < numMembers; i++) {
      GroupMember m = createGroupMember("node" + i, "group" + (i / 3));
      members.add(m);
      Session<?> s = createSession(m);
      sessions.add(s);
      term = elector.enter(createEnterOp(partId, m, s)).getTerm();
    }

    // Check current primary.
    assertEquals(1L, term.getTerm());
    assertEquals(members.get(0), term.getPrimary());
    assertEquals(numMembers, term.getCandidatesList().size());
    assertEquals(members.get(3), term.getCandidatesList().get(1));
    assertEquals(members.get(6), term.getCandidatesList().get(2));

    // Expire session of primary and check new term.
    // New primary should be the first of the old backups.
    elector.onExpire(sessions.get(0));
    term = elector.getTerm(createGetTermOp(partId, members.get(3), sessions.get(3))).getTerm();
    assertEquals(2L, term.getTerm());
    assertEquals(members.get(3), term.getPrimary());
    assertEquals(numMembers - 1, term.getCandidatesList().size());
    assertEquals(members.get(6), term.getCandidatesList().get(1));
    assertEquals(members.get(1), term.getCandidatesList().get(2));

    // Expire session of backup and check term updated.
    elector.onExpire(sessions.get(6));
    term = elector.getTerm(createGetTermOp(partId, members.get(5), sessions.get(5))).getTerm();
    assertEquals(2L, term.getTerm());
    assertEquals(members.get(3), term.getPrimary());
    assertEquals(numMembers - 2, term.getCandidatesList().size());
    assertEquals(members.get(1), term.getCandidatesList().get(1));
    assertEquals(members.get(4), term.getCandidatesList().get(2));
  }

  @Test
  public void testSortCandidatesByGroup() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;

    term = enter("node1", "group1", elector);
    assertEquals("node1", term.getPrimary().getMemberId());

    term = enter("node2", "group1", elector);
    assertEquals("node1", term.getPrimary().getMemberId());
    assertEquals("node2", term.getCandidatesList().get(1).getMemberId());

    term = enter("node3", "group1", elector);
    assertEquals("node1", term.getPrimary().getMemberId());
    assertEquals("node2", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node3", term.getCandidatesList().get(2).getMemberId());

    term = enter("node4", "group2", elector);
    assertEquals("node1", term.getPrimary().getMemberId());
    assertEquals("node4", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node2", term.getCandidatesList().get(2).getMemberId());

    term = enter("node5", "group3", elector);
    assertEquals("node1", term.getPrimary().getMemberId());
    assertEquals("node4", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node5", term.getCandidatesList().get(2).getMemberId());

    term = enter("node6", "group3", elector);
    assertEquals("node1", term.getPrimary().getMemberId());
    assertEquals("node4", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node5", term.getCandidatesList().get(2).getMemberId());

    assertEquals("node1", term.getCandidatesList().get(0).getMemberId());
    assertEquals("node4", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node5", term.getCandidatesList().get(2).getMemberId());
    assertEquals("node2", term.getCandidatesList().get(3).getMemberId());
    assertEquals("node6", term.getCandidatesList().get(4).getMemberId());
    assertEquals("node3", term.getCandidatesList().get(5).getMemberId());
  }

  @Test
  public void testSortCandidatesWithoutGroup() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;

    term = enter("node1", "node1", elector);
    term = enter("node2", "node2", elector);
    term = enter("node3", "node3", elector);
    term = enter("node4", "node4", elector);
    term = enter("node5", "node5", elector);
    term = enter("node6", "node6", elector);

    assertEquals("node1", term.getCandidatesList().get(0).getMemberId());
    assertEquals("node2", term.getCandidatesList().get(1).getMemberId());
    assertEquals("node3", term.getCandidatesList().get(2).getMemberId());
    assertEquals("node4", term.getCandidatesList().get(3).getMemberId());
    assertEquals("node5", term.getCandidatesList().get(4).getMemberId());
    assertEquals("node6", term.getCandidatesList().get(5).getMemberId());
  }

  private PrimaryTerm enter(String nodeId, String groupId, PrimaryElectorService elector) {
    PartitionId partId = PartitionId.newBuilder()
        .setGroup("test")
        .setPartition(1)
        .build();
    GroupMember member = createGroupMember(nodeId, groupId);
    Session session = createSession(member);
    return elector.enter(createEnterOp(partId, member, session)).getTerm();
  }

  Commit<EnterRequest> createEnterOp(PartitionId partition, GroupMember member, Session<?> session) {
    EnterRequest enter = EnterRequest.newBuilder().setPartitionId(partition).setMember(member).build();
    return new DefaultCommit<>(0, null, enter, session, System.currentTimeMillis());
  }

  Commit<GetTermRequest> createGetTermOp(PartitionId partition, GroupMember member, Session<?> session) {
    GetTermRequest getTerm = GetTermRequest.newBuilder().setPartitionId(partition).build();
    return new DefaultCommit<>(0, null, getTerm, session, System.currentTimeMillis());
  }

  GroupMember createGroupMember(String id, String groupId) {
    return GroupMember.newBuilder()
        .setMemberId(id)
        .setMemberGroupId(groupId)
        .build();
  }

  PrimaryElectorService newService() {
    PrimaryElectorService elector = new PrimaryElectorService();
    elector.init(new ServiceContext() {
      @Override
      public PrimitiveId serviceId() {
        return PrimitiveId.from(1L);
      }

      @Override
      public String serviceName() {
        return "test-primary-elector";
      }

      @SuppressWarnings("rawtypes")
      @Override
      public PrimitiveType serviceType() {
        return PrimaryElectorType.instance();
      }

      @Override
      public MemberId localMemberId() {
        return null;
      }

      @Override
      public Role role() {
        return null;
      }

      @Override
      public long currentIndex() {
        return 0;
      }

      @Override
      public Session<?> currentSession() {
        return null;
      }

      @Override
      public OperationType currentOperation() {
        return null;
      }

      @Override
      public LogicalClock logicalClock() {
        return null;
      }

      @Override
      public WallClock wallClock() {
        return null;
      }
    });
    elector.tick(WallClockTimestamp.from(System.currentTimeMillis()));
    return elector;
  }

  @SuppressWarnings("rawtypes")
  Session<?> createSession(final GroupMember member) {
    return new Session() {
      long sessionId = sessionNum++;

      @Override
      public SessionId sessionId() {
        return SessionId.from(sessionId);
      }

      @Override
      public String primitiveName() {
        return null; // not used in test
      }

      @Override
      public PrimitiveType primitiveType() {
        return null; // not used in test
      }

      @Override
      public MemberId memberId() {
        return MemberId.from(member.getMemberId());
      }

      @Override
      public State getState() {
        return State.OPEN;
      }

      @Override
      public void publish(EventType eventType, Object event) {
        // not used in test
      }

      @Override
      public void publish(PrimitiveEvent event) {
        // not used in test
      }

      @Override
      public void accept(Consumer event) {
        // not used in test
      }

      @Override
      public String toString() {
        return "Session " + sessionId;
      }
    };
  }
}
