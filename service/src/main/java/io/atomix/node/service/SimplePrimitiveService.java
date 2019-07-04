/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.node.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.node.service.impl.DefaultServiceExecutor;
import io.atomix.node.service.impl.DefaultServiceScheduler;
import io.atomix.node.service.impl.ServiceCodec;
import io.atomix.node.service.operation.CommandId;
import io.atomix.node.service.operation.QueryId;
import io.atomix.node.service.protocol.CommandRequest;
import io.atomix.node.service.protocol.CommandResponse;
import io.atomix.node.service.protocol.QueryRequest;
import io.atomix.node.service.protocol.QueryResponse;
import io.atomix.node.service.protocol.ResponseContext;
import io.atomix.node.service.protocol.StreamContext;
import io.atomix.node.service.protocol.StreamResponse;
import io.atomix.node.service.util.ByteArrayDecoder;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.EncodingStreamHandler;
import io.atomix.utils.stream.StreamHandler;

/**
 * Simple primitive service.
 */
public abstract class SimplePrimitiveService extends AbstractPrimitiveService implements PrimitiveService {
    private AbstractPrimitiveService.Context context;
    private DefaultServiceExecutor executor;
    private DefaultServiceScheduler scheduler;
    private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();

    @Override
    public void init(StateMachine.Context context) {
        this.context = new AbstractPrimitiveService.Context(context);
        this.executor = new DefaultServiceExecutor(new ServiceCodec(), this.context.getLogger());
        this.scheduler = new DefaultServiceScheduler(this.context);
        super.init(this.context, scheduler, scheduler);
        configure(executor);
    }

    @Override
    public boolean canDelete(long index) {
        return true;
    }

    @Override
    public CompletableFuture<byte[]> apply(Command<byte[]> command) {
        return applyCommand(command.map(bytes -> ByteArrayDecoder.decode(bytes, CommandRequest::parseFrom)))
            .thenApply(CommandResponse::toByteArray);
    }

    private CompletableFuture<CommandResponse> applyCommand(Command<CommandRequest> command) {
        // Run tasks scheduled to execute prior to this command.
        scheduler.runScheduledTasks(command.timestamp());

        CommandId<?, ?> commandId = new CommandId<>(command.value().getName());
        OperationExecutor operation = executor.getExecutor(commandId);

        byte[] output;
        try {
            output = operation.execute(command.value().getCommand().toByteArray());
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        } finally {
            // Run tasks pending to be executed after this command.
            scheduler.runPendingTasks();
        }

        return CompletableFuture.completedFuture(CommandResponse.newBuilder()
            .setContext(ResponseContext.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setOutput(ByteString.copyFrom(output))
            .build());
    }

    @Override
    public CompletableFuture<Void> apply(Command<byte[]> command, StreamHandler<byte[]> handler) {
        return applyCommand(command.map(bytes -> ByteArrayDecoder.decode(bytes, CommandRequest::parseFrom)), new StreamHandler<StreamResponse>() {
            @Override
            public void next(StreamResponse response) {
                handler.next(response.toByteArray());
            }

            @Override
            public void complete() {
                handler.complete();
            }

            @Override
            public void error(Throwable error) {
                handler.error(error);
            }
        });
    }

    private CompletableFuture<Void> applyCommand(Command<CommandRequest> command, StreamHandler<StreamResponse> handler) {
        // Run tasks scheduled to execute prior to this command.
        scheduler.runScheduledTasks(command.timestamp());

        CommandId<?, ?> commandId = new CommandId<>(command.value().getName());
        OperationExecutor operation = executor.getExecutor(commandId);
        try {
            operation.execute(
                command.value().getCommand().toByteArray(),
                new EncodingStreamHandler<byte[], StreamResponse>(handler, value -> StreamResponse.newBuilder()
                    .setContext(StreamContext.newBuilder()
                        .setIndex(getCurrentIndex())
                        .build())
                    .setOutput(ByteString.copyFrom(value))
                    .build()));
        } finally {
            // Run tasks pending to be executed after this command.
            scheduler.runPendingTasks();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> apply(Query<byte[]> query) {
        return applyQuery(query.map(bytes -> ByteArrayDecoder.decode(bytes, QueryRequest::parseFrom)))
            .thenApply(QueryResponse::toByteArray);
    }

    private CompletableFuture<QueryResponse> applyQuery(Query<QueryRequest> query) {
        CompletableFuture<QueryResponse> future = new CompletableFuture<>();
        if (query.value().getContext().getIndex() > getCurrentIndex()) {
            indexQueries.computeIfAbsent(query.value().getContext().getIndex(), index -> new LinkedList<>())
                .add(() -> applyQuery(query, future));
        } else {
            applyQuery(query, future);
        }
        return future;
    }

    private void applyQuery(Query<QueryRequest> request, CompletableFuture<QueryResponse> future) {
        QueryId<?, ?> queryId = new QueryId<>(request.value().getName());
        OperationExecutor operation = executor.getExecutor(queryId);

        byte[] output;
        try {
            output = operation.execute(request.value().getQuery().toByteArray());
        } catch (Exception e) {
            future.completeExceptionally(e);
            return;
        }

        future.complete(QueryResponse.newBuilder()
            .setContext(ResponseContext.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setOutput(ByteString.copyFrom(output))
            .build());
    }

    @Override
    public CompletableFuture<Void> apply(Query<byte[]> query, StreamHandler<byte[]> handler) {
        return applyQuery(query.map(bytes -> ByteArrayDecoder.decode(bytes, QueryRequest::parseFrom)), new StreamHandler<StreamResponse>() {
            @Override
            public void next(StreamResponse response) {
                handler.next(response.toByteArray());
            }

            @Override
            public void complete() {
                handler.complete();
            }

            @Override
            public void error(Throwable error) {
                handler.error(error);
            }
        });
    }

    private CompletableFuture<Void> applyQuery(Query<QueryRequest> query, StreamHandler<StreamResponse> handler) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (query.value().getContext().getIndex() > getCurrentIndex()) {
            indexQueries.computeIfAbsent(query.value().getContext().getIndex(), index -> new LinkedList<>())
                .add(() -> applyQuery(query, handler, future));
        } else {
            applyQuery(query, handler, future);
        }
        return future;
    }

    private void applyQuery(Query<QueryRequest> query, StreamHandler<StreamResponse> handler, CompletableFuture<Void> future) {
        QueryId<?, ?> queryId = new QueryId<>(query.value().getName());
        OperationExecutor operation = executor.getExecutor(queryId);
        operation.execute(
            query.value().getQuery().toByteArray(),
            new EncodingStreamHandler<byte[], StreamResponse>(handler, value -> StreamResponse.newBuilder()
                .setContext(StreamContext.newBuilder()
                    .setIndex(getCurrentIndex())
                    .build())
                .setOutput(ByteString.copyFrom(value))
                .build()));
        future.complete(null);
    }
}
