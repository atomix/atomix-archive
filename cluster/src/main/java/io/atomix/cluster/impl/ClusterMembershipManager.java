/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.VersionService;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.cluster.protocol.GroupMembershipEvent;
import io.atomix.cluster.protocol.GroupMembershipEventListener;
import io.atomix.cluster.protocol.GroupMembershipProtocol;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.event.AbstractListenerManager;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Default cluster implementation.
 */
@Component
public class ClusterMembershipManager
    extends AbstractListenerManager<ClusterMembershipEvent, ClusterMembershipEventListener>
    implements ClusterMembershipService, Managed {

  private static final Logger LOGGER = getLogger(ClusterMembershipManager.class);

  @Dependency
  private MemberService memberService;

  @Dependency
  private VersionService versionService;

  @Dependency
  private NodeDiscoveryService discoveryService;

  @Dependency
  private GroupMembershipProtocol protocol;

  private StatefulMember localMember;
  private final GroupMembershipEventListener membershipEventListener = this::handleMembershipEvent;

  public ClusterMembershipManager() {
  }

  @VisibleForTesting
  public ClusterMembershipManager(
      MemberService memberService,
      VersionService versionService,
      NodeDiscoveryService discoveryService,
      GroupMembershipProtocol protocol) {
    this.memberService = memberService;
    this.versionService = versionService;
    this.discoveryService = discoveryService;
    this.protocol = protocol;
    start();
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return protocol.getMembers();
  }

  @Override
  public Member getMember(MemberId memberId) {
    return protocol.getMember(memberId);
  }

  /**
   * Handles a group membership event.
   */
  private void handleMembershipEvent(GroupMembershipEvent event) {
    post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.valueOf(event.type().name()), event.member()));
  }

  @Override
  public CompletableFuture<Void> start() {
    this.localMember = new StatefulMember(
        memberService.getLocalMember().id(),
        memberService.getLocalMember().address(),
        memberService.getLocalMember().zone(),
        memberService.getLocalMember().rack(),
        memberService.getLocalMember().host(),
        memberService.getLocalMember().properties(),
        versionService.version());
    localMember.setActive(true);
    localMember.setReachable(true);
    protocol.addListener(membershipEventListener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    localMember.setActive(false);
    localMember.setReachable(false);
    protocol.removeListener(membershipEventListener);
    LOGGER.info("Stopped");
    return CompletableFuture.completedFuture(null);
  }
}
