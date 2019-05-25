package io.atomix.cluster.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Strings;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.VersionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Local member manager.
 */
@Component(MemberConfig.class)
public class MemberManager implements MemberService, Managed<MemberConfig> {

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
  public CompletableFuture<Void> start(MemberConfig config) {
    this.localMember = Member.newBuilder()
        .setId(config.getId().id())
        .setNamespace(config.getId().namespace())
        .setHost(config.getHost())
        .setPort(config.getPort())
        .setHostId(Strings.nullToEmpty(config.getHostId()))
        .setRackId(Strings.nullToEmpty(config.getRackId()))
        .setZoneId(Strings.nullToEmpty(config.getZoneId()))
        .putAllProperties((Map) config.getProperties())
        .setVersion(versionService.version().toString())
        .build();
    return CompletableFuture.completedFuture(null);
  }
}
