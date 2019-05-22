package io.atomix.core.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.core.election.AnointRequest;
import io.atomix.core.election.AnointResponse;
import io.atomix.core.election.CloseRequest;
import io.atomix.core.election.CloseResponse;
import io.atomix.core.election.CreateRequest;
import io.atomix.core.election.CreateResponse;
import io.atomix.core.election.ElectionId;
import io.atomix.core.election.EnterRequest;
import io.atomix.core.election.EnterResponse;
import io.atomix.core.election.EventRequest;
import io.atomix.core.election.EventResponse;
import io.atomix.core.election.EvictRequest;
import io.atomix.core.election.EvictResponse;
import io.atomix.core.election.GetLeadershipRequest;
import io.atomix.core.election.GetLeadershipResponse;
import io.atomix.core.election.KeepAliveRequest;
import io.atomix.core.election.KeepAliveResponse;
import io.atomix.core.election.LeaderElectionServiceGrpc;
import io.atomix.core.election.PromoteRequest;
import io.atomix.core.election.PromoteResponse;
import io.atomix.core.election.WithdrawRequest;
import io.atomix.core.election.WithdrawResponse;
import io.atomix.core.election.impl.LeaderElectionProxy;
import io.atomix.core.election.impl.LeaderElectionService;
import io.atomix.core.election.impl.ListenResponse;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionResponseHeader;
import io.atomix.grpc.headers.SessionStreamHeader;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * gRPC leader election service implementation.
 */
public class LeaderElectionServiceImpl extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
  private final PrimitiveFactory<LeaderElectionProxy, ElectionId> primitiveFactory;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionHeader, CreateRequest, CreateResponse> create;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionHeader, KeepAliveRequest, KeepAliveResponse> keepAlive;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionHeader, CloseRequest, CloseResponse> close;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, EnterRequest, EnterResponse> enter;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, WithdrawRequest, WithdrawResponse> withdraw;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, AnointRequest, AnointResponse> anoint;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, PromoteRequest, PromoteResponse> promote;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, EvictRequest, EvictResponse> evict;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionQueryHeader, GetLeadershipRequest, GetLeadershipResponse> getLeadership;
  private final RequestExecutor<LeaderElectionProxy, ElectionId, SessionCommandHeader, EventRequest, EventResponse> events;

  public LeaderElectionServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        LeaderElectionService.TYPE,
        (id, client) -> new LeaderElectionProxy(new DefaultSessionClient(id, client)),
        ELECTION_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, CloseResponse::getDefaultInstance);
    this.enter = new RequestExecutor<>(primitiveFactory, ENTER_DESCRIPTOR, EnterResponse::getDefaultInstance);
    this.withdraw = new RequestExecutor<>(primitiveFactory, WITHDRAW_DESCRIPTOR, WithdrawResponse::getDefaultInstance);
    this.anoint = new RequestExecutor<>(primitiveFactory, ANOINT_DESCRIPTOR, AnointResponse::getDefaultInstance);
    this.promote = new RequestExecutor<>(primitiveFactory, PROMOTE_DESCRIPTOR, PromoteResponse::getDefaultInstance);
    this.evict = new RequestExecutor<>(primitiveFactory, EVICT_DESCRIPTOR, EvictResponse::getDefaultInstance);
    this.getLeadership = new RequestExecutor<>(primitiveFactory, GET_LEADERSHIP_DESCRIPTOR, GetLeadershipResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENTS_DESCRIPTOR, EventResponse::getDefaultInstance);
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, sessionId, lock) -> lock.openSession(OpenSessionRequest.newBuilder()
            .setSessionId(sessionId)
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> CreateResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(sessionId)
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    close.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void enter(EnterRequest request, StreamObserver<EnterResponse> responseObserver) {
    enter.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.enter(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.EnterRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> EnterResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setTerm(response.getRight().getTerm())
                .setTimestamp(response.getRight().getTimestamp())
                .setLeader(response.getRight().getLeader())
                .addAllCandidates(response.getRight().getCandidatesList())
                .build()));
  }

  @Override
  public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> responseObserver) {
    withdraw.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.withdraw(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.WithdrawRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> WithdrawResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void anoint(AnointRequest request, StreamObserver<AnointResponse> responseObserver) {
    anoint.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.anoint(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.AnointRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> AnointResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void promote(PromoteRequest request, StreamObserver<PromoteResponse> responseObserver) {
    promote.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.promote(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.PromoteRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> PromoteResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void evict(EvictRequest request, StreamObserver<EvictResponse> responseObserver) {
    evict.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.evict(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.EvictRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> EvictResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void getLeadership(GetLeadershipRequest request, StreamObserver<GetLeadershipResponse> responseObserver) {
    getLeadership.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.getLeadership(
            SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.election.impl.GetLeadershipRequest.newBuilder().build())
            .thenApply(response -> GetLeadershipResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .setTerm(response.getRight().getTerm())
                .setTimestamp(response.getRight().getTimestamp())
                .setLeader(response.getRight().getLeader())
                .addAllCandidates(response.getRight().getCandidatesList())
                .build()));
  }

  @Override
  public void events(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, handler, election) -> election.listen(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.election.impl.ListenRequest.newBuilder().build(), handler),
        (partitionId, header, response) -> EventResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(header.getPartitionId())
                .setIndex(response.getLeft().getIndex())
                .setSequenceNumber(response.getLeft().getSequence())
                .addStreams(SessionStreamHeader.newBuilder()
                    .setStreamId(response.getLeft().getStreamId())
                    .setIndex(response.getLeft().getIndex())
                    .setLastItemNumber(response.getLeft().getSequence())
                    .build())
                .build())
            .setType(EventResponse.Type.valueOf(response.getRight().getType().name()))
            .setTerm(response.getRight().getTerm())
            .setTimestamp(response.getRight().getTimestamp())
            .setLeader(response.getRight().getLeader())
            .addAllCandidates(response.getRight().getCandidatesList())
            .build());
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<ElectionId> ELECTION_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<ElectionId>() {
    @Override
    public String getName(ElectionId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(ElectionId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(ElectionId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(ElectionId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(ElectionId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(ElectionId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(ElectionId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<CreateRequest, ElectionId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, ElectionId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<CloseRequest, ElectionId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<EnterRequest, ElectionId, SessionCommandHeader> ENTER_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EnterRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<WithdrawRequest, ElectionId, SessionCommandHeader> WITHDRAW_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(WithdrawRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<AnointRequest, ElectionId, SessionCommandHeader> ANOINT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(AnointRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<PromoteRequest, ElectionId, SessionCommandHeader> PROMOTE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(PromoteRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<EvictRequest, ElectionId, SessionCommandHeader> EVICT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EvictRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<GetLeadershipRequest, ElectionId, SessionQueryHeader> GET_LEADERSHIP_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(GetLeadershipRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<EventRequest, ElectionId, SessionCommandHeader> EVENTS_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EventRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
