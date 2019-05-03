package io.atomix.cluster.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;

/**
 * Local member manager.
 */
@Component(MemberConfig.class)
public class MemberManager implements MemberService, Managed<MemberConfig> {
  private Member localMember;

  private MemberManager() {
  }

  public MemberManager(Member localMember) {
    this.localMember = localMember;
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public CompletableFuture<Void> start(MemberConfig config) {
    this.localMember = new Member(config);
    return CompletableFuture.completedFuture(null);
  }
}
