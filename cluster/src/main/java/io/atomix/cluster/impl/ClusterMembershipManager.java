package io.atomix.cluster.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterMessage;
import io.atomix.cluster.GossipMessage;
import io.atomix.cluster.JoinRequest;
import io.atomix.cluster.JoinResponse;
import io.atomix.cluster.LeaveRequest;
import io.atomix.cluster.LeaveResponse;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberEvent;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.MembershipConfig;
import io.atomix.cluster.MembershipServiceGrpc;
import io.atomix.cluster.ProbeRequest;
import io.atomix.cluster.ProbeResponse;
import io.atomix.cluster.VerifyRequest;
import io.atomix.cluster.VerifyResponse;
import io.atomix.cluster.discovery.DiscoveryEvent;
import io.atomix.cluster.discovery.Node;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.cluster.grpc.ChannelService;
import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * gRPC cluster membership server.
 */
@Component(MembershipConfig.class)
public class ClusterMembershipManager extends MembershipServiceGrpc.MembershipServiceImplBase implements ClusterMembershipService, Managed<MembershipConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembershipManager.class);

  @Dependency
  private ServiceRegistry serviceRegistry;
  @Dependency
  private ChannelService channelService;
  @Dependency
  private MemberService memberService;
  @Dependency
  private NodeDiscoveryService discoveryService;

  private MembershipConfig config;

  private Member localMember;
  private final Map<MemberId, Member> members = Maps.newConcurrentMap();
  private final Map<MemberId, MemberStream> streams = Maps.newConcurrentMap();
  private List<Member> randomMembers = Lists.newCopyOnWriteArrayList();
  private final Consumer<DiscoveryEvent> discoveryEventListener = this::handleDiscoveryEvent;
  private final Map<MemberId, Member> updates = new LinkedHashMap<>();
  private final Set<Consumer<MemberEvent>> eventListeners = new CopyOnWriteArraySet<>();

  private final AtomicInteger probeCounter = new AtomicInteger();

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-membership", LOGGER));
  private ScheduledFuture<?> gossipFuture;
  private ScheduledFuture<?> probeFuture;

  ClusterMembershipManager() {
  }

  ClusterMembershipManager(
      ServiceRegistry serviceRegistry,
      ChannelService channelService,
      MemberService memberService,
      NodeDiscoveryService discoveryService) {
    this.serviceRegistry = serviceRegistry;
    this.channelService = channelService;
    this.memberService = memberService;
    this.discoveryService = discoveryService;
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return ImmutableSet.copyOf(members.values());
  }

  @Override
  public Member getMember(MemberId memberId) {
    return members.get(memberId);
  }

  private void post(MemberEvent event) {
    eventListeners.forEach(l -> l.accept(event));
  }

  @Override
  public void addListener(Consumer<MemberEvent> listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<MemberEvent> listener) {
    eventListeners.remove(listener);
  }

  private MemberId getMemberId(Member member) {
    return MemberId.from(member);
  }

  private Member copy(Member member) {
    return Member.newBuilder(member).build();
  }

  private Member copy(Member member, Function<Member.Builder, Member.Builder> update) {
    return update.apply(Member.newBuilder(member)).build();
  }

  private MemberStream getStream(Member member) {
    MemberId memberId = getMemberId(member);
    return streams.computeIfAbsent(memberId, id -> {
      MembershipServiceGrpc.MembershipServiceStub stub = MembershipServiceGrpc.newStub(channelService.getChannel(member.getHost(), member.getPort()));
      MemberStream s = new MemberStream(memberId, null);
      s.stream = stub.join(s);
      return s;
    });
  }

  /**
   * Updates the state for the given member.
   *
   * @param member the member for which to update the state
   * @return whether the state for the member was updated
   */
  private boolean updateState(Member member) {
    // If the member matches the local member, ignore the update.
    if (getMemberId(member).equals(getMemberId(localMember))) {
      return false;
    }

    Member swimMember = members.get(getMemberId(member));

    // If the local member is not present, add the member in the ALIVE state.
    if (swimMember == null) {
      if (member.getState() == Member.State.ALIVE) {
        swimMember = copy(member);
        members.put(getMemberId(swimMember), swimMember);
        randomMembers.add(swimMember);
        Collections.shuffle(randomMembers);
        LOGGER.debug("{} - Member added {}", getLocalMemberId(), swimMember);
        swimMember = copy(swimMember, builder -> builder.setState(Member.State.ALIVE));
        post(MemberEvent.newBuilder()
            .setType(MemberEvent.Type.ADDED)
            .setMember(copy(swimMember))
            .build());
        recordUpdate(copy(swimMember));
        return true;
      }
      return false;
    }
    // If the term has been increased, update the member and record a gossip event.
    else if (member.getIncarnationNumber() > swimMember.getIncarnationNumber()) {
      // If the member's version has changed, remove the old member and add the new member.
      if (!Objects.equals(member.getVersion(), swimMember.getVersion())) {
        members.remove(MemberId.from(member.getId(), member.getNamespace()));
        randomMembers.remove(swimMember);
        post(MemberEvent.newBuilder()
            .setType(MemberEvent.Type.REMOVED)
            .setMember(copy(swimMember))
            .build());
        swimMember = copy(member, builder -> builder.setState(Member.State.ALIVE));
        members.put(getMemberId(member), swimMember);
        randomMembers.add(swimMember);
        Collections.shuffle(randomMembers);
        LOGGER.debug("{} - Evicted member for new version {}", getLocalMember(), swimMember);
        post(MemberEvent.newBuilder()
            .setType(MemberEvent.Type.ADDED)
            .setMember(copy(swimMember))
            .build());
        recordUpdate(copy(swimMember));
      } else {
        // Update the term for the local member.
        swimMember = copy(swimMember, builder -> builder.setIncarnationNumber(member.getIncarnationNumber()));

        // If the state has been changed to ALIVE, trigger a REACHABILITY_CHANGED event and then update metadata.
        if (member.getState() == Member.State.ALIVE && swimMember.getState() != Member.State.ALIVE) {
          swimMember = copy(swimMember, builder -> builder.setState(Member.State.ALIVE));
          LOGGER.debug("{} - Member reachable {}", getLocalMemberId(), swimMember);
        }
        // If the state has been changed to SUSPECT, update metadata and then trigger a REACHABILITY_CHANGED event.
        else if (member.getState() == Member.State.SUSPECT && swimMember.getState() != Member.State.SUSPECT) {
          swimMember = copy(swimMember, builder -> builder.setState(Member.State.SUSPECT));
          LOGGER.debug("{} - Member unreachable {}", getLocalMemberId(), swimMember);
          if (config.getNotifySuspect()) {
            gossip(swimMember, Lists.newArrayList(copy(swimMember)));
          }
        }
        // If the state has been changed to DEAD, trigger a REACHABILITY_CHANGED event if necessary and then remove
        // the member from the members list and trigger a MEMBER_REMOVED event.
        else if (member.getState() == Member.State.DEAD && swimMember.getState() != Member.State.DEAD) {
          if (swimMember.getState() == Member.State.ALIVE) {
            swimMember = copy(swimMember, builder -> builder.setState(Member.State.SUSPECT));
            LOGGER.debug("{} - Member unreachable {}", getLocalMemberId(), swimMember);
          }
          swimMember = copy(swimMember, builder -> builder.setState(Member.State.DEAD));
          members.remove(getMemberId(swimMember));
          randomMembers.remove(swimMember);
          Collections.shuffle(randomMembers);
          LOGGER.debug("{} - Member removed {}", getLocalMemberId(), swimMember);
          post(MemberEvent.newBuilder()
              .setType(MemberEvent.Type.REMOVED)
              .setMember(copy(swimMember))
              .build());
        }

        // Always enqueue an update for gossip when the term changes.
        recordUpdate(copy(swimMember));
        return true;
      }
    }
    // If the term remained the same but the state has progressed, update the state and trigger events.
    else if (member.getIncarnationNumber() == swimMember.getIncarnationNumber() && member.getState().ordinal() > swimMember.getState().ordinal()) {
      swimMember = copy(swimMember, builder -> builder.setState(member.getState()));

      // If the updated state is SUSPECT, post a REACHABILITY_CHANGED event and record an update.
      if (member.getState() == Member.State.SUSPECT) {
        LOGGER.debug("{} - Member unreachable {}", getLocalMemberId(), swimMember);
        if (config.getNotifySuspect()) {
          gossip(swimMember, Lists.newArrayList(copy(swimMember)));
        }
      }
      // If the updated state is DEAD, post a REACHABILITY_CHANGED event if necessary, then post a MEMBER_REMOVED
      // event and record an update.
      else if (member.getState() == Member.State.DEAD) {
        members.remove(getMemberId(swimMember));
        randomMembers.remove(swimMember);
        Collections.shuffle(randomMembers);
        LOGGER.debug("{} - Member removed {}", getLocalMemberId(), swimMember);
        post(MemberEvent.newBuilder()
            .setType(MemberEvent.Type.REMOVED)
            .setMember(copy(swimMember))
            .build());
      }
      recordUpdate(copy(swimMember));
      return true;
    }
    return false;
  }

  /**
   * Records an update as an immutable member.
   *
   * @param member the updated member
   */
  private void recordUpdate(Member member) {
    updates.put(getMemberId(member), member);
  }

  /**
   * Checks suspect nodes for failures.
   */
  private void checkFailures() {
    for (Member member : members.values()) {
      Duration timeout = Duration.ofSeconds(config.getFailureTimeout().getSeconds())
          .plusNanos(config.getFailureTimeout().getNanos());
      if (member.getState() == Member.State.SUSPECT && System.currentTimeMillis() - member.getTimestamp() > timeout.toMillis()) {
        member = copy(member, builder -> builder.setState(Member.State.DEAD));
        members.remove(getMemberId(member));
        randomMembers.remove(member);
        Collections.shuffle(randomMembers);
        LOGGER.debug("{} - Member removed {}", getLocalMemberId(), member);
        post(MemberEvent.newBuilder()
            .setType(MemberEvent.Type.REMOVED)
            .setMember(copy(member))
            .build());
        recordUpdate(copy(member));
      }
    }
  }

  /**
   * Synchronizes the node state with peers.
   */
  private void sync() {
    List<Member> syncMembers = discoveryService.getNodes().stream()
        .map(node -> Member.newBuilder()
            .setId(node.getId())
            .setNamespace(node.getNamespace())
            .setHost(node.getHost())
            .setPort(node.getPort())
            .build())
        .filter(member -> !getMemberId(member).equals(getMemberId(localMember)))
        .collect(Collectors.toList());
    for (Member member : syncMembers) {
      sync(copy(member));
    }
  }

  /**
   * Synchronizes the node state with the given peer.
   *
   * @param member the peer with which to synchronize the node state
   */
  private void sync(Member member) {
    LOGGER.trace("{} - Synchronizing membership with {}", getLocalMemberId(), member);
    getStream(member).sendJoinRequest(JoinRequest.newBuilder()
        .setMember(copy(localMember))
        .build());
  }

  /**
   * Handles a sync request from the given member.
   *
   * @param member the member from which to handle the sync request
   * @return the collection of members in the cluster
   */
  private Collection<Member> handleSync(Member member) {
    updateState(member);
    return members.values().stream()
        .map(this::copy)
        .collect(Collectors.toList());
  }

  /**
   * Handles a member leaving the cluster.
   *
   * @param member the member to remove
   */
  private void handleLeave(Member member) {
    updateState(copy(member, builder -> builder.setState(Member.State.DEAD)
        .setIncarnationNumber(member.getIncarnationNumber() + 1)));
  }

  /**
   * Sends probes to all members or to the next member in round robin fashion.
   */
  private void probe() {
    // First get a sorted list of discovery service nodes that are not present in the SWIM members.
    // This is necessary to ensure we attempt to probe all nodes that are provided by the discovery provider.
    List<Member> probeMembers = Lists.newArrayList(discoveryService.getNodes().stream()
        .map(node -> Member.newBuilder()
            .setId(node.getId())
            .setNamespace(node.getNamespace())
            .setHost(node.getHost())
            .setPort(node.getPort())
            .build())
        .filter(member -> !members.containsKey(getMemberId(member)))
        .filter(member -> !getMemberId(member).equals(getMemberId(localMember)))
        .sorted(Comparator.comparing(this::getMemberId))
        .collect(Collectors.toList()));

    // Then add the randomly sorted list of SWIM members.
    probeMembers.addAll(randomMembers);

    // If there are members to probe, select the next member to probe using a counter for round robin probes.
    if (!probeMembers.isEmpty()) {
      Member probeMember = probeMembers.get(Math.abs(probeCounter.incrementAndGet() % probeMembers.size()));
      probe(Member.newBuilder(probeMember).build());
    }
  }

  /**
   * Probes the given member.
   *
   * @param member the member to probe
   */
  private void probe(Member member) {
    LOGGER.trace("{} - Probing {}", getLocalMemberId(), member);
    getStream(member).sendProbeRequest(ProbeRequest.newBuilder()
        .setSource(copy(localMember))
        .setTarget(copy(member))
        .build())
        .thenAccept(result -> {
          if (result.isPresent()) {
            updateState(result.get());
          } else {
            LOGGER.debug("{} - Failed to probe {}", getMemberId(this.localMember), member);
            // Verify that the local member term has not changed and request probes from peers.
            Member swimMember = members.get(getMemberId(member));
            if (swimMember != null && swimMember.getIncarnationNumber() == member.getIncarnationNumber()) {
              requestProbes(copy(swimMember));
            }
          }
        });
  }

  /**
   * Handles a probe from another peer.
   *
   * @param remoteMember the probing member info
   * @param localMember  the local member info
   * @return the current term
   */
  private Member handleProbe(Member remoteMember, Member localMember) {
    LOGGER.trace("{} - Received probe {} from {}", getMemberId(this.localMember), localMember, remoteMember);

    // If the probe indicates a term greater than the local term, update the local term, increment and respond.
    if (localMember.getIncarnationNumber() > this.localMember.getIncarnationNumber()) {
      this.localMember = Member.newBuilder(this.localMember)
          .setIncarnationNumber(localMember.getIncarnationNumber())
          .build();
      if (config.getBroadcastDisputes()) {
        broadcast(Member.newBuilder(this.localMember).build());
      }
    }
    // If the probe indicates this member is suspect, increment the local term and respond.
    else if (localMember.getState() == Member.State.SUSPECT) {
      this.localMember = copy(this.localMember, builder -> builder.setIncarnationNumber(this.localMember.getIncarnationNumber() + 1));
      if (config.getBroadcastDisputes()) {
        broadcast(copy(this.localMember));
      }
    }

    // Update the state of the probing member.
    updateState(remoteMember);
    return copy(this.localMember);
  }

  /**
   * Requests probes from n peers.
   */
  private void requestProbes(Member suspect) {
    Collection<Member> members = selectRandomMembers(config.getSuspectProbes() - 1, suspect);
    if (!members.isEmpty()) {
      AtomicInteger counter = new AtomicInteger();
      AtomicBoolean succeeded = new AtomicBoolean();
      for (Member member : members) {
        requestProbe(member, suspect).whenCompleteAsync((success, error) -> {
          int count = counter.incrementAndGet();
          if (error == null && success) {
            succeeded.set(true);
          }
          // If the count is equal to the number of probe peers and no probe has succeeded, the node is unreachable.
          else if (count == members.size() && !succeeded.get()) {
            failProbes(suspect);
          }
        }, scheduler);
      }
    } else {
      failProbes(suspect);
    }
  }

  /**
   * Marks the given member suspect after all probes failing.
   */
  private void failProbes(Member suspect) {
    Member swimMember = Member.newBuilder(suspect).build();
    LOGGER.debug("{} - Failed all probes of {}", getMemberId(localMember), swimMember);
    swimMember = Member.newBuilder(suspect)
        .setState(Member.State.SUSPECT)
        .build();
    if (updateState(swimMember) && config.getBroadcastUpdates()) {
      broadcast(copy(swimMember));
    }
  }

  /**
   * Requests a probe of the given suspect from the given member.
   *
   * @param member  the member to perform the probe
   * @param suspect the suspect member to probe
   */
  private CompletableFuture<Boolean> requestProbe(Member member, Member suspect) {
    LOGGER.debug("{} - Requesting probe of {} from {}", getMemberId(this.localMember), suspect, member);
    return getStream(member).sendVerifyRequest(VerifyRequest.newBuilder()
        .setTarget(suspect)
        .build())
        .thenApply(succeeded -> {
          LOGGER.debug("{} - Probe request of {} from {} {}", getLocalMemberId(), suspect, member, succeeded ? "succeeded" : "failed");
          return succeeded;
        });
  }

  /**
   * Selects a set of random members, excluding the local member and a given member.
   *
   * @param count   count the number of random members to select
   * @param exclude the member to exclude
   * @return members a set of random members
   */
  private Collection<Member> selectRandomMembers(int count, Member exclude) {
    List<Member> members = this.members.values().stream()
        .filter(member -> !getMemberId(member).equals(getMemberId(localMember)) && !getMemberId(member).equals(getMemberId(exclude)))
        .collect(Collectors.toList());
    Collections.shuffle(members);
    return members.subList(0, Math.min(members.size(), count));
  }

  /**
   * Handles a probe request.
   *
   * @param member the member to probe
   */
  private CompletableFuture<Boolean> handleProbeRequest(Member member) {
    LOGGER.trace("{} - Probing {}", getLocalMemberId(), member);
    return getStream(member).sendProbeRequest(ProbeRequest.newBuilder()
        .setSource(copy(localMember))
        .setTarget(member)
        .build())
        .thenApplyAsync(result -> {
          if (!result.isPresent()) {
            LOGGER.debug("{} - Failed to probe {}", getLocalMemberId(), member);
            return false;
          }
          return true;
        }, scheduler);
  }

  /**
   * Broadcasts the given update to all peers.
   *
   * @param update the update to broadcast
   */
  private void broadcast(Member update) {
    for (Member member : members.values()) {
      if (!getMemberId(localMember).equals(getMemberId(member))) {
        gossip(member, Lists.newArrayList(update));
      }
    }
  }

  /**
   * Gossips pending updates to the cluster.
   */
  private void gossip() {
    // Check suspect nodes for failure timeouts.
    checkFailures();

    // Copy and clear the list of pending updates.
    if (!updates.isEmpty()) {
      List<Member> updates = Lists.newArrayList(this.updates.values());
      this.updates.clear();

      // Gossip the pending updates to peers.
      gossip(updates);
    }
  }

  /**
   * Gossips this node's pending updates with a random set of peers.
   *
   * @param updates a collection of updated to gossip
   */
  private void gossip(Collection<Member> updates) {
    // Get a list of available peers. If peers are available, randomize the peer list and select a subset of
    // peers with which to gossip updates.
    List<Member> members = Lists.newArrayList(randomMembers);
    if (!members.isEmpty()) {
      Collections.shuffle(members);
      for (int i = 0; i < Math.min(members.size(), config.getGossipFanout()); i++) {
        gossip(members.get(i), updates);
      }
    }
  }

  /**
   * Gossips this node's pending updates with the given peer.
   *
   * @param member  the peer with which to gossip this node's updates
   * @param updates the updated members to gossip
   */
  private void gossip(Member member, Collection<Member> updates) {
    LOGGER.trace("{} - Gossipping updates {} to {}", getMemberId(localMember), updates, member);
    getStream(member).sendGossipMessage(GossipMessage.newBuilder()
        .addAllUpdates(updates)
        .build());
  }

  /**
   * Handles a gossip message from a peer.
   */
  private void handleGossipUpdates(Collection<Member> updates) {
    for (Member update : updates) {
      updateState(update);
    }
  }

  /**
   * Handles a member location event.
   *
   * @param event the member location event
   */
  private void handleDiscoveryEvent(DiscoveryEvent event) {
    switch (event.getType()) {
      case JOIN:
        handleJoinEvent(event.getNode());
        break;
      case LEAVE:
        handleLeaveEvent(event.getNode());
        break;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Handles a node join event.
   */
  private void handleJoinEvent(Node node) {
    Member member = Member.newBuilder()
        .setId(node.getId())
        .setNamespace(node.getNamespace())
        .setHost(node.getHost())
        .setPort(node.getPort())
        .build();
    if (!members.containsKey(MemberId.from(member.getId(), member.getNamespace()))) {
      probe(member);
    }
  }

  /**
   * Handles a node leave event.
   */
  private void handleLeaveEvent(Node node) {
    Member member = members.get(MemberId.from(node.getId(), node.getNamespace()));
    if (member != null && member.getState() == Member.State.DEAD) {
      members.remove(MemberId.from(node.getId(), node.getNamespace()));
    }
  }

  @Override
  public StreamObserver<ClusterMessage> join(StreamObserver<ClusterMessage> responseObserver) {
    return new MemberStream(null, responseObserver);
  }

  @Override
  public CompletableFuture<Void> start(MembershipConfig config) {
    this.config = config;
    this.localMember = copy(memberService.getLocalMember());
    serviceRegistry.register(this);
    discoveryService.addListener(discoveryEventListener);

    LOGGER.info("{} - Member activated: {}", getLocalMemberId(), localMember);
    localMember = copy(localMember, builder -> builder.setState(Member.State.ALIVE));
    members.put(getMemberId(localMember), localMember);
    post(MemberEvent.newBuilder()
        .setType(MemberEvent.Type.ADDED)
        .setMember(copy(localMember))
        .build());

    Duration gossipInterval = Duration.ofSeconds(config.getGossipInterval().getSeconds())
        .plusNanos(config.getGossipInterval().getNanos());
    gossipFuture = scheduler.scheduleAtFixedRate(
        this::gossip, 0, gossipInterval.toMillis(), TimeUnit.MILLISECONDS);

    Duration probeInterval = Duration.ofSeconds(config.getProbeInterval().getSeconds())
        .plusNanos(config.getProbeInterval().getNanos());
    probeFuture = scheduler.scheduleAtFixedRate(
        this::probe, 0, probeInterval.toMillis(), TimeUnit.MILLISECONDS);
    scheduler.execute(this::sync);
    LOGGER.info("Started");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    gossipFuture.cancel(false);
    probeFuture.cancel(false);
    scheduler.shutdownNow();
    LOGGER.info("{} - Member deactivated: {}", getLocalMemberId(), localMember);
    localMember = copy(localMember, builder -> builder.setState(Member.State.DEAD));
    broadcast(localMember);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Member stream.
   */
  private class MemberStream implements StreamObserver<ClusterMessage> {
    private volatile StreamObserver<ClusterMessage> stream;
    private volatile MemberId memberId;
    private final Queue<CompletableFuture<Optional<Member>>> probeFutures = new LinkedList<>();
    private final Map<MemberId, Queue<CompletableFuture<Boolean>>> verifyFutures = new ConcurrentHashMap<>();

    MemberStream(MemberId memberId, StreamObserver<ClusterMessage> stream) {
      this.memberId = memberId;
      this.stream = stream;
    }

    void sendJoinRequest(JoinRequest request) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setJoinRequest(request)
            .build());
      }
    }

    void sendJoinResponse(JoinResponse response) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setJoinResponse(response)
            .build());
      }
    }

    void sendGossipMessage(GossipMessage message) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setGossip(message)
            .build());
      }
    }

    CompletableFuture<Optional<Member>> sendProbeRequest(ProbeRequest request) {
      CompletableFuture<Optional<Member>> future = new CompletableFuture<>();
      synchronized (probeFutures) {
        probeFutures.add(future);
      }
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setProbeRequest(request)
            .build());
      }
      return future;
    }

    void sendProbeResponse(ProbeResponse response) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setProbeResponse(response)
            .build());
      }
    }

    CompletableFuture<Boolean> sendVerifyRequest(VerifyRequest request) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      Queue<CompletableFuture<Boolean>> futures = verifyFutures.computeIfAbsent(getMemberId(request.getTarget()), id -> new LinkedList<>());
      synchronized (futures) {
        futures.add(future);
      }
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setVerifyRequest(request)
            .build());
      }
      return future;
    }

    void sendVerifyResponse(VerifyResponse response) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setVerifyResponse(response)
            .build());
      }
    }

    void sendLeaveRequest(LeaveRequest request) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setLeaveRequest(request)
            .build());
      }
    }

    void sendLeaveResponse(LeaveResponse response) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onNext(ClusterMessage.newBuilder()
            .setLeaveResponse(response)
            .build());
      }
    }

    void handleJoinRequest(JoinRequest request) {
      Collection<Member> members = handleSync(request.getMember());
      sendJoinResponse(JoinResponse.newBuilder()
          .addAllMembers(members)
          .build());
    }

    void handleJoinResponse(JoinResponse response) {
      response.getMembersList().forEach(ClusterMembershipManager.this::updateState);
    }

    void handleGossipMessage(GossipMessage message) {
      handleGossipUpdates(message.getUpdatesList());
    }

    void handleProbeRequest(ProbeRequest request) {
      Member member = handleProbe(request.getSource(), request.getTarget());
      sendProbeResponse(ProbeResponse.newBuilder()
          .setMember(member)
          .build());
    }

    void handleProbeResponse(ProbeResponse response) {
      CompletableFuture<Optional<Member>> future;
      synchronized (probeFutures) {
        future = probeFutures.poll();
      }
      if (future != null) {
        future.complete(Optional.of(response.getMember()));
      }
    }

    void handleVerifyRequest(VerifyRequest request) {
      ClusterMembershipManager.this.handleProbeRequest(request.getTarget())
          .thenAccept(succeeded -> sendVerifyResponse(VerifyResponse.newBuilder()
              .setTarget(request.getTarget())
              .setSucceeded(succeeded)
              .build()));
    }

    void handleVerifyResponse(VerifyResponse response) {
      Queue<CompletableFuture<Boolean>> futures = verifyFutures.get(getMemberId(response.getTarget()));
      if (futures != null) {
        CompletableFuture<Boolean> future;
        synchronized (futures) {
          future = futures.poll();
        }
        if (future != null) {
          future.complete(response.getSucceeded());
        }
      }
    }

    void handleLeaveRequest(LeaveRequest request) {
      handleLeave(request.getMember());
      sendLeaveResponse(LeaveResponse.newBuilder().build());
    }

    void handleLeaveResponse(LeaveResponse response) {

    }

    @Override
    public void onNext(ClusterMessage value) {
      if (value.hasJoinRequest() && memberId == null) {
        JoinRequest request = value.getJoinRequest();
        memberId = getMemberId(request.getMember());
        MemberStream stream = streams.put(memberId, this);
        if (stream != null) {
          stream.onCompleted();
          stream.close();
        }
      }

      if (value.hasJoinRequest()) {
        handleJoinRequest(value.getJoinRequest());
      } else if (value.hasJoinResponse()) {
        handleJoinResponse(value.getJoinResponse());
      } else if (value.hasGossip()) {
        handleGossipMessage(value.getGossip());
      } else if (value.hasProbeRequest()) {
        handleProbeRequest(value.getProbeRequest());
      } else if (value.hasProbeResponse()) {
        handleProbeResponse(value.getProbeResponse());
      } else if (value.hasVerifyRequest()) {
        handleVerifyRequest(value.getVerifyRequest());
      } else if (value.hasVerifyResponse()) {
        handleVerifyResponse(value.getVerifyResponse());
      } else if (value.hasLeaveRequest()) {
        handleLeaveRequest(value.getLeaveRequest());
      } else if (value.hasLeaveResponse()) {
        handleLeaveResponse(value.getLeaveResponse());
      }
    }

    @Override
    public void onError(Throwable t) {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onCompleted();
        this.stream = null;
      }

      Queue<CompletableFuture<Optional<Member>>> futures;
      synchronized (probeFutures) {
        futures = new LinkedList<>(probeFutures);
        probeFutures.clear();
      }
      futures.forEach(future -> future.complete(Optional.empty()));

      for (Queue<CompletableFuture<Boolean>> queue : verifyFutures.values()) {
        Queue<CompletableFuture<Boolean>> queueFutures;
        synchronized (queue) {
          queueFutures = new LinkedList<>(queue);
          queue.clear();
        }
        queueFutures.forEach(future -> future.completeExceptionally(t));
      }
      if (memberId != null) {
        streams.remove(memberId, this);
      }
    }

    @Override
    public void onCompleted() {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onCompleted();
        this.stream = null;
      }

      Queue<CompletableFuture<Optional<Member>>> futures;
      synchronized (probeFutures) {
        futures = new LinkedList<>(probeFutures);
        probeFutures.clear();
      }
      futures.forEach(future -> future.complete(Optional.empty()));

      for (Queue<CompletableFuture<Boolean>> queue : verifyFutures.values()) {
        Queue<CompletableFuture<Boolean>> queueFutures;
        synchronized (queue) {
          queueFutures = new LinkedList<>(queue);
          queue.clear();
        }
        queueFutures.forEach(future -> future.complete(null));
      }
      if (memberId != null) {
        streams.remove(memberId, this);
      }
    }

    void close() {
      StreamObserver<ClusterMessage> stream = this.stream;
      if (stream != null) {
        stream.onCompleted();
      }
    }
  }
}
