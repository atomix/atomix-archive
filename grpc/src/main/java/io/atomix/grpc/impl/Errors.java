/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.grpc.impl;

import java.util.function.Supplier;

import com.google.protobuf.Message;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

/**
 * gRPC errors.
 */
public final class Errors {

  /**
   * Sends a failure response to the given observer.
   *
   * @param status the response status
   * @param message the failure message
   * @param responseSupplier the default response supplier
   * @param responseObserver the response observer on which to send the error
   */
  public static void fail(Status status, String message, Supplier<? extends Message> responseSupplier, StreamObserver<? extends Message> responseObserver) {
    Message response = responseSupplier.get();
    Metadata.Key key = ProtoUtils.keyForProto(response);
    Metadata metadata = new Metadata();
    metadata.put(key, response);
    responseObserver.onError(status.withDescription(message)
        .asRuntimeException(metadata));
  }

  private Errors() {
  }
}
