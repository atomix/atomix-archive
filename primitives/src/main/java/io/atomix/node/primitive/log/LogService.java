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
package io.atomix.node.primitive.log;

import java.util.function.Consumer;

import io.atomix.api.log.ConsumeRequest;
import io.atomix.api.log.LogRecord;
import io.atomix.api.log.LogServiceGrpc;
import io.atomix.api.log.ProduceRequest;
import io.atomix.api.log.ProduceResponse;
import io.atomix.node.service.client.LogClient;
import io.grpc.stub.StreamObserver;

/**
 * Log service implementation.
 */
public class LogService extends LogServiceGrpc.LogServiceImplBase {
    private final LogClient client;

    public LogService(LogClient client) {
        this.client = client;
    }

    @Override
    public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responseObserver) {
        return new StreamObserver<ProduceRequest>() {
            @Override
            public void onNext(ProduceRequest request) {
                client.producer().append(request.getValue().toByteArray())
                    .whenComplete((response, error) -> {
                        if (error != null) {
                            responseObserver.onError(error);
                        }
                    });
            }

            @Override
            public void onError(Throwable error) {
                responseObserver.onError(error);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void consume(ConsumeRequest request, StreamObserver<LogRecord> responseObserver) {
        Consumer<io.atomix.node.service.protocol.LogRecord> consumer = record -> {
            responseObserver.onNext(LogRecord.newBuilder()
                .setOffset(record.getIndex())
                .setTimestamp(record.getTimestamp())
                .setValue(record.getValue())
                .build());
        };

        client.consumer().consume(request.getOffset(), consumer);
    }
}
