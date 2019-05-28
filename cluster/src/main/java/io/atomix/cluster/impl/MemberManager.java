package io.atomix.cluster.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.VersionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Local member manager.
 */
@Component(Member.class)
public class MemberManager implements MemberService, Managed<Member> {

  @Dependency
  private VersionService versionService;

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
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start(Member config) {
    this.localMember = Member.newBuilder(config).build();
    return CompletableFuture.completedFuture(null);
  }
}
