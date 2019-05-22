/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.partition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.impl.NodeMemberGroup;

/**
 * Member group strategy.
 * <p>
 * Member group strategies are default implementations of {@link MemberGroupProvider} for built-in node attributes.
 */
public enum MemberGroupStrategy implements MemberGroupProvider {

  /**
   * Zone aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique zone in the cluster.
   */
  ZONE_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> !Strings.isNullOrEmpty(node.getZoneId())
          ? node.getZoneId()
          : MemberId.from(node.getId(), node.getNamespace()).toString());
    }
  },

  /**
   * Rack aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique rack in the cluster.
   */
  RACK_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> !Strings.isNullOrEmpty(node.getRackId())
          ? node.getRackId()
          : MemberId.from(node.getId(), node.getNamespace()).toString());
    }
  },

  /**
   * Host aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique host in the cluster.
   */
  HOST_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> !Strings.isNullOrEmpty(node.getHostId())
          ? node.getHostId()
          : MemberId.from(node.getId(), node.getNamespace()).toString());
    }
  },

  /**
   * Node aware member group strategy (the default).
   * <p>
   * This strategy will create a member group for each node in the cluster, effectively behaving the same as if
   * no member groups were defined.
   */
  NODE_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> MemberId.from(node.getId(), node.getNamespace()).toString());
    }
  };

  /**
   * Groups nodes by the given key function.
   *
   * @param members     the nodes to group
   * @param keyFunction the key function to apply to nodes to extract a key
   * @return a collection of node member groups
   */
  protected Collection<MemberGroup> groupNodes(Collection<Member> members, Function<Member, String> keyFunction) {
    Map<String, Set<Member>> groups = new HashMap<>();
    for (Member member : members) {
      groups.computeIfAbsent(keyFunction.apply(member), k -> new HashSet<>()).add(member);
    }

    return groups.entrySet().stream()
        .map(entry -> new NodeMemberGroup(MemberGroupId.from(entry.getKey()), entry.getValue()))
        .collect(Collectors.toList());
  }
}
