package io.atomix.server.election;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.api.election.LeaderElectionServiceGrpc;
import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * gRPC leader election service implementation.
 */
public class LeaderElectionServiceImpl extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
  private final PrimitiveFactory<LeaderElectionProxy> primitiveFactory;
  private final RequestExecutor<LeaderElectionProxy, SessionHeader, io.atomix.api.election.CreateRequest, io.atomix.api.election.CreateResponse> create;
  private final RequestExecutor<LeaderElectionProxy, SessionHeader, io.atomix.api.election.KeepAliveRequest, io.atomix.api.election.KeepAliveResponse> keepAlive;
  private final RequestExecutor<LeaderElectionProxy, SessionHeader, io.atomix.api.election.CloseRequest, io.atomix.api.election.CloseResponse> close;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.EnterRequest, io.atomix.api.election.EnterResponse> enter;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.WithdrawRequest, io.atomix.api.election.WithdrawResponse> withdraw;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.AnointRequest, io.atomix.api.election.AnointResponse> anoint;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.PromoteRequest, io.atomix.api.election.PromoteResponse> promote;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.EvictRequest, io.atomix.api.election.EvictResponse> evict;
  private final RequestExecutor<LeaderElectionProxy, SessionQueryHeader, io.atomix.api.election.GetLeadershipRequest, io.atomix.api.election.GetLeadershipResponse> getLeadership;
  private final RequestExecutor<LeaderElectionProxy, SessionCommandHeader, io.atomix.api.election.EventRequest, io.atomix.api.election.EventResponse> events;

  public LeaderElectionServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        LeaderElectionService.TYPE,
        (id, client) -> new LeaderElectionProxy(new DefaultSessionClient(id, client)));
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.election.CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, io.atomix.api.election.KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, io.atomix.api.election.CloseResponse::getDefaultInstance);
    this.enter = new RequestExecutor<>(primitiveFactory, ENTER_DESCRIPTOR, io.atomix.api.election.EnterResponse::getDefaultInstance);
    this.withdraw = new RequestExecutor<>(primitiveFactory, WITHDRAW_DESCRIPTOR, io.atomix.api.election.WithdrawResponse::getDefaultInstance);
    this.anoint = new RequestExecutor<>(primitiveFactory, ANOINT_DESCRIPTOR, io.atomix.api.election.AnointResponse::getDefaultInstance);
    this.promote = new RequestExecutor<>(primitiveFactory, PROMOTE_DESCRIPTOR, io.atomix.api.election.PromoteResponse::getDefaultInstance);
    this.evict = new RequestExecutor<>(primitiveFactory, EVICT_DESCRIPTOR, io.atomix.api.election.EvictResponse::getDefaultInstance);
    this.getLeadership = new RequestExecutor<>(primitiveFactory, GET_LEADERSHIP_DESCRIPTOR, io.atomix.api.election.GetLeadershipResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENTS_DESCRIPTOR, io.atomix.api.election.EventResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.election.CreateRequest request, StreamObserver<io.atomix.api.election.CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, election) -> election.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> io.atomix.api.election.CreateResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(response.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(io.atomix.api.election.KeepAliveRequest request, StreamObserver<io.atomix.api.election.KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.keepAlive(KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> io.atomix.api.election.KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void close(io.atomix.api.election.CloseRequest request, StreamObserver<io.atomix.api.election.CloseResponse> responseObserver) {
    close.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.closeSession(CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()).thenApply(response -> io.atomix.api.election.CloseResponse.newBuilder().build()));
  }

  @Override
  public void enter(io.atomix.api.election.EnterRequest request, StreamObserver<io.atomix.api.election.EnterResponse> responseObserver) {
    enter.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.enter(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            EnterRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> io.atomix.api.election.EnterResponse.newBuilder()
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
  public void withdraw(io.atomix.api.election.WithdrawRequest request, StreamObserver<io.atomix.api.election.WithdrawResponse> responseObserver) {
    withdraw.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.withdraw(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            WithdrawRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> io.atomix.api.election.WithdrawResponse.newBuilder()
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
  public void anoint(io.atomix.api.election.AnointRequest request, StreamObserver<io.atomix.api.election.AnointResponse> responseObserver) {
    anoint.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.anoint(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            AnointRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> io.atomix.api.election.AnointResponse.newBuilder()
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
  public void promote(io.atomix.api.election.PromoteRequest request, StreamObserver<io.atomix.api.election.PromoteResponse> responseObserver) {
    promote.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.promote(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            PromoteRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> io.atomix.api.election.PromoteResponse.newBuilder()
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
  public void evict(io.atomix.api.election.EvictRequest request, StreamObserver<io.atomix.api.election.EvictResponse> responseObserver) {
    evict.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.evict(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            EvictRequest.newBuilder()
                .setId(request.getCandidateId())
                .build())
            .thenApply(response -> io.atomix.api.election.EvictResponse.newBuilder()
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
  public void getLeadership(io.atomix.api.election.GetLeadershipRequest request, StreamObserver<io.atomix.api.election.GetLeadershipResponse> responseObserver) {
    getLeadership.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, election) -> election.getLeadership(
            SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            GetLeadershipRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.election.GetLeadershipResponse.newBuilder()
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
  public void events(io.atomix.api.election.EventRequest request, StreamObserver<io.atomix.api.election.EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, handler, election) -> election.listen(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(), handler),
        (partitionId, header, response) -> io.atomix.api.election.EventResponse.newBuilder()
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
            .setType(io.atomix.api.election.EventResponse.Type.valueOf(response.getRight().getType().name()))
            .setTerm(response.getRight().getTerm())
            .setTimestamp(response.getRight().getTimestamp())
            .setLeader(response.getRight().getLeader())
            .addAllCandidates(response.getRight().getCandidatesList())
            .build());
  }

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.CreateRequest, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.election.CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.KeepAliveRequest, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.election.KeepAliveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.CloseRequest, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.election.CloseRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.EnterRequest, SessionCommandHeader> ENTER_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.EnterRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.WithdrawRequest, SessionCommandHeader> WITHDRAW_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.WithdrawRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.AnointRequest, SessionCommandHeader> ANOINT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.AnointRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.PromoteRequest, SessionCommandHeader> PROMOTE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.PromoteRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.EvictRequest, SessionCommandHeader> EVICT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.EvictRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.GetLeadershipRequest, SessionQueryHeader> GET_LEADERSHIP_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.election.GetLeadershipRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.election.EventRequest, SessionCommandHeader> EVENTS_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.election.EventRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
