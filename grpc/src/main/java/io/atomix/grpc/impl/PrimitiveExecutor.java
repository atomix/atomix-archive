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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.atomix.core.Atomix;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.protocol.LogCompatibleBuilder;
import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.counter.CounterCompatibleBuilder;
import io.atomix.primitive.protocol.counter.CounterProtocol;
import io.atomix.primitive.protocol.map.MapCompatibleBuilder;
import io.atomix.primitive.protocol.map.MapProtocol;
import io.atomix.primitive.protocol.set.SetCompatibleBuilder;
import io.atomix.primitive.protocol.set.SetProtocol;
import io.atomix.primitive.protocol.value.ValueCompatibleBuilder;
import io.atomix.primitive.protocol.value.ValueProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.gossip.AntiEntropyProtocol;
import io.atomix.protocols.gossip.CrdtProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

/**
 * Primitive helpers.
 */
public class PrimitiveExecutor<S extends SyncPrimitive, A extends AsyncPrimitive> {
  private static final String ID_FIELD = "id";
  private static final String NAME_FIELD = "name";
  private static final String RAFT_PROTOCOL = "raft";
  private static final String MULTI_PRIMARY_PROTOCOL = "multi_primary";
  private static final String LOG_PROTOCOL = "log";
  private static final String GOSSIP_PROTOCOL = "gossip";
  private static final String CRDT_PROTOCOL = "crdt";

  private final Atomix atomix;
  private final PrimitiveType<?, ?, ?> type;
  private final Function<S, A> converter;

  public PrimitiveExecutor(Atomix atomix, PrimitiveType<?, ?, ?> type, Function<S, A> converter) {
    this.atomix = atomix;
    this.type = type;
    this.converter = converter;
  }

  /**
   * Executes the given function on the primitive.
   *
   * @param request          the primitive request
   * @param function         the function to execute
   * @param responseObserver the response observer
   */
  @SuppressWarnings("unchecked")
  public <T extends Message, U extends Message> void execute(
      T request,
      Supplier<U> responseSupplier,
      StreamObserver<U> responseObserver,
      Function<A, CompletableFuture<U>> function) {
    if (isValidRequest(request, responseSupplier, responseObserver)) {
      getPrimitive(getId(request)).whenComplete((primitive, getError) -> {
        if (getError == null) {
          function.apply(primitive).whenComplete((result, funcError) -> {
            if (funcError == null) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(funcError);
              responseObserver.onCompleted();
            }
          });
        } else {
          responseObserver.onError(getError);
          responseObserver.onCompleted();
        }
      });
    }
  }

  /**
   * Validates the given request.
   *
   * @param request          the request to validate
   * @param responseSupplier the default response supplier
   * @param responseObserver the response observer
   * @return indicates whether the request is valid
   */
  public <T extends Message, U extends Message> boolean isValidRequest(T request, Supplier<U> responseSupplier, StreamObserver<U> responseObserver) {
    if (!hasId(request)) {
      fail(Status.INVALID_ARGUMENT, "Primitive ID not specified", responseSupplier, responseObserver);
      return false;
    }
    return isValidId(getId(request), responseSupplier, responseObserver);
  }

  /**
   * Validates the given ID.
   *
   * @param id               the primitive ID
   * @param responseSupplier the default response supplier
   * @param responseObserver the response observer
   * @return indicates whetehr the ID is valid
   */
  public <T extends Message, U extends Message> boolean isValidId(T id, Supplier<U> responseSupplier, StreamObserver<U> responseObserver) {
    if (!hasName(id)) {
      fail(Status.INVALID_ARGUMENT, "Primitive name not specified", responseSupplier, responseObserver);
      return false;
    }
    if (!hasProtocol(id)) {
      fail(Status.INVALID_ARGUMENT, "Primitive protocol not specified", responseSupplier, responseObserver);
      return false;
    }
    return true;
  }

  /**
   * Returns a boolean indicating whether the request has an ID defined.
   *
   * @param request the request to validate
   * @return indicates whether the request has an ID defined
   */
  private boolean hasId(Message request) {
    return request.hasField(request.getDescriptorForType().findFieldByName(ID_FIELD));
  }

  /**
   * Returns a boolean indicating whether the given ID has a valid name.
   *
   * @param id the ID to validate
   * @return indicates whether the ID has a valid name
   */
  private boolean hasName(Message id) {
    String name = getName(id);
    return name == null || !name.equals("");
  }

  /**
   * Returns a boolean indicating whether the given ID has a valid protocol defined.
   *
   * @param id the ID to validate
   * @return indicates whether the request has a valid protocol defined
   */
  private boolean hasProtocol(Message id) {
    for (Descriptors.OneofDescriptor oneof : id.getDescriptorForType().getOneofs()) {
      if (id.hasOneof(oneof)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the primitive with the given ID.
   *
   * @param id the primitive ID
   * @return a future to be completed with the primitive instance
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<A> getPrimitive(Message id) {
    String name = getName(id);
    PrimitiveProtocol protocol = toProtocol(id);

    // If the primitive name is null, set the name to the partition group name if a log protocol is configured.
    // Distributed log primitives are unnamed, but a name is required to create the primitive.
    if (protocol instanceof LogProtocol && name == null) {
      name = ((LogProtocol) protocol).group();
    }

    PrimitiveBuilder builder = atomix.primitiveBuilder(name, type);
    if (protocol instanceof LogProtocol) {
      if (builder instanceof LogCompatibleBuilder) {
        ((LogCompatibleBuilder) builder).withProtocol((LogProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    } else if (protocol instanceof ProxyProtocol) {
      if (builder instanceof ProxyCompatibleBuilder) {
        ((ProxyCompatibleBuilder) builder).withProtocol((ProxyProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    } else if (protocol instanceof MapProtocol) {
      if (builder instanceof MapCompatibleBuilder) {
        ((MapCompatibleBuilder) builder).withProtocol((MapProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    } else if (protocol instanceof CounterProtocol) {
      if (builder instanceof CounterCompatibleBuilder) {
        ((CounterCompatibleBuilder) builder).withProtocol((CounterProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    } else if (protocol instanceof SetProtocol) {
      if (builder instanceof SetCompatibleBuilder) {
        ((SetCompatibleBuilder) builder).withProtocol((SetProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    } else if (protocol instanceof ValueProtocol) {
      if (builder instanceof ValueCompatibleBuilder) {
        ((ValueCompatibleBuilder) builder).withProtocol((ValueProtocol) protocol);
      } else {
        throw new AssertionError();
      }
    }
    return builder.getAsync().thenApply(primitive -> converter.apply((S) primitive));
  }

  /**
   * Returns the message ID for the given request.
   *
   * @param request the request for which to get the ID
   * @return the message ID
   */
  private static Message getId(Message request) {
    Descriptors.FieldDescriptor idField = request.getDescriptorForType().findFieldByName(ID_FIELD);
    return (Message) request.getField(idField);
  }

  /**
   * Returns the primitive name for the given ID.
   *
   * @param id the primitive ID
   * @return the primitive name
   */
  private static String getName(Message id) {
    Descriptors.FieldDescriptor nameField = id.getDescriptorForType().findFieldByName(NAME_FIELD);
    return nameField != null ? (String) id.getField(nameField) : null;
  }

  /**
   * Converts the given primitive Id to a protocol.
   *
   * @param id the primitive ID
   * @return the primitive protocol
   */
  private static PrimitiveProtocol toProtocol(Message id) {
    Descriptors.FieldDescriptor raftField = id.getDescriptorForType().findFieldByName(RAFT_PROTOCOL);
    if (raftField != null && id.hasField(raftField)) {
      String group = ((io.atomix.grpc.protocol.MultiRaftProtocol) id.getField(raftField)).getGroup();
      return MultiRaftProtocol.builder(group)
          .build();
    }

    Descriptors.FieldDescriptor multiPrimaryField = id.getDescriptorForType().findFieldByName(MULTI_PRIMARY_PROTOCOL);
    if (multiPrimaryField != null && id.hasField(multiPrimaryField)) {
      String group = ((io.atomix.grpc.protocol.MultiPrimaryProtocol) id.getField(multiPrimaryField)).getGroup();
      return MultiPrimaryProtocol.builder(group)
          .build();
    }

    Descriptors.FieldDescriptor logField = id.getDescriptorForType().findFieldByName(LOG_PROTOCOL);
    if (logField != null && id.hasField(logField)) {
      String group = ((io.atomix.grpc.protocol.DistributedLogProtocol) id.getField(logField)).getGroup();
      return DistributedLogProtocol.builder(group)
          .build();
    }

    Descriptors.FieldDescriptor gossipField = id.getDescriptorForType().findFieldByName(GOSSIP_PROTOCOL);
    if (gossipField != null && id.hasField(gossipField)) {
      return AntiEntropyProtocol.builder().build();
    }

    Descriptors.FieldDescriptor crdtField = id.getDescriptorForType().findFieldByName(CRDT_PROTOCOL);
    if (crdtField != null && id.hasField(crdtField)) {
      return CrdtProtocol.builder().build();
    }
    return null;
  }

  /**
   * Sends a failure response to the given observer.
   *
   * @param status           the response status
   * @param message          the failure message
   * @param responseSupplier the default response supplier
   * @param responseObserver the response observer on which to send the error
   */
  private static <R extends Message> void fail(Status status, String message, Supplier<R> responseSupplier, StreamObserver<R> responseObserver) {
    R response = responseSupplier.get();
    Metadata.Key<R> key = ProtoUtils.keyForProto(response);
    Metadata metadata = new Metadata();
    metadata.put(key, response);
    responseObserver.onError(status.withDescription(message)
        .asRuntimeException(metadata));
  }
}
