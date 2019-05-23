package io.atomix.core.impl;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionGroupMembership;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.grpc.Channel;

/**
 * Primitive builder for core primitives.
 */
public abstract class AbstractPrimitiveBuilder<B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig, P extends SyncPrimitive> extends PrimitiveBuilder<B, C, P> {
  public AbstractPrimitiveBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }

  /**
   * Returns a channel factory for the primitive.
   *
   * @return the channel factory
   */
  protected Supplier<Channel> getChannelFactory() {
    String group = ((ServiceProtocol) protocol()).group();
    AtomicInteger counter = new AtomicInteger();
    return () -> {
      PartitionGroupMembership membership = managementService.getGroupMembershipService().getMembership(group);
      List<Member> members = managementService.getMembershipService().getMembers().stream()
          .filter(member -> membership.members().contains(MemberId.from(member)))
          .sorted(Comparator.comparing(MemberId::from))
          .collect(Collectors.toList());
      if (members.isEmpty()) {
        return null;
      }

      Member member = members.get(counter.incrementAndGet() % members.size());
      return managementService.getGrpcService().getChannel(member.getHost(), member.getPort());
    };
  }
}
