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
package io.atomix.core.set.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.collect.Sets;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.component.Component;
import io.atomix.utils.stream.StreamHandler;

/**
 * Set service.
 */
public class SetService extends AbstractSetService {
  public static final Type TYPE = new Type();

  /**
   * Set service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "set";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new SetService();
    }
  }

  private Set<String> set = Sets.newConcurrentHashSet();
  private Set<String> lockedElements = new ConcurrentSkipListSet<>();
  private Map<String, DistributedSetTransaction> transactions = new HashMap<>();

  @Override
  public SizeResponse size(SizeRequest request) {
    return SizeResponse.newBuilder()
        .setSize(set.size())
        .build();
  }

  @Override
  public ContainsResponse contains(ContainsRequest request) {
    boolean contains = request.getValuesCount() == 0
        || request.getValuesCount() == 1 ? set.contains(request.getValues(0)) : set.containsAll(request.getValuesList());
    return ContainsResponse.newBuilder()
        .setContains(contains)
        .build();
  }

  @Override
  public AddResponse add(AddRequest request) {
    if (request.getValuesCount() == 0) {
      return AddResponse.newBuilder()
          .setStatus(UpdateStatus.NOOP)
          .setAdded(false)
          .build();
    } else if (request.getValuesCount() == 1) {
      String value = request.getValues(0);
      if (lockedElements.contains(value)) {
        return AddResponse.newBuilder()
            .setStatus(UpdateStatus.WRITE_LOCK)
            .setAdded(false)
            .build();
      } else {
        boolean added = set.add(value);
        if (added) {
          onEvent(ListenResponse.newBuilder()
              .setType(ListenResponse.Type.ADDED)
              .setValue(value)
              .build());
        }
        return AddResponse.newBuilder()
            .setStatus(UpdateStatus.OK)
            .setAdded(added)
            .build();
      }
    } else {
      for (String value : request.getValuesList()) {
        if (lockedElements.contains(value)) {
          return AddResponse.newBuilder()
              .setStatus(UpdateStatus.WRITE_LOCK)
              .setAdded(false)
              .build();
        }
      }
      boolean added = set.addAll(request.getValuesList());
      if (added) {
        for (String value : request.getValuesList()) {
          onEvent(ListenResponse.newBuilder()
              .setType(ListenResponse.Type.ADDED)
              .setValue(value)
              .build());
        }
      }
      return AddResponse.newBuilder()
          .setStatus(UpdateStatus.OK)
          .setAdded(added)
          .build();
    }
  }

  @Override
  public RemoveResponse remove(RemoveRequest request) {
    if (request.getValuesCount() == 0) {
      return RemoveResponse.newBuilder()
          .setStatus(UpdateStatus.NOOP)
          .setRemoved(false)
          .build();
    } else if (request.getValuesCount() == 1) {
      String value = request.getValues(0);
      if (lockedElements.contains(value)) {
        return RemoveResponse.newBuilder()
            .setStatus(UpdateStatus.WRITE_LOCK)
            .setRemoved(false)
            .build();
      } else {
        boolean removed = set.remove(value);
        if (removed) {
          onEvent(ListenResponse.newBuilder()
              .setType(ListenResponse.Type.REMOVED)
              .setValue(value)
              .build());
        }
        return RemoveResponse.newBuilder()
            .setStatus(UpdateStatus.OK)
            .setRemoved(removed)
            .build();
      }
    } else {
      for (String value : request.getValuesList()) {
        if (lockedElements.contains(value)) {
          return RemoveResponse.newBuilder()
              .setStatus(UpdateStatus.WRITE_LOCK)
              .setRemoved(false)
              .build();
        }
      }
      boolean removed = set.removeAll(request.getValuesList());
      if (removed) {
        for (String value : request.getValuesList()) {
          onEvent(ListenResponse.newBuilder()
              .setType(ListenResponse.Type.REMOVED)
              .setValue(value)
              .build());
        }
      }
      return RemoveResponse.newBuilder()
          .setStatus(UpdateStatus.OK)
          .setRemoved(removed)
          .build();
    }
  }

  @Override
  public ClearResponse clear(ClearRequest request) {
    for (String value : set) {
      onEvent(ListenResponse.newBuilder()
          .setType(ListenResponse.Type.REMOVED)
          .setValue(value)
          .build());
    }
    set.clear();
    return ClearResponse.newBuilder().build();
  }

  @Override
  public void listen(ListenRequest request, StreamHandler<ListenResponse> handler) {
    // Keep the stream open.
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    // Complete the stream.
    StreamHandler<ListenResponse> stream = getCurrentSession().getStream(request.getStreamId());
    if (stream != null) {
      stream.complete();
    }
    return UnlistenResponse.newBuilder().build();
  }

  @Override
  public void iterate(IterateRequest request, StreamHandler<IterateResponse> handler) {
    for (String value : set) {
      handler.next(IterateResponse.newBuilder()
          .setValue(value)
          .build());
    }
    handler.complete();
  }

  @Override
  public PrepareResponse prepare(PrepareRequest request) {
    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      if (lockedElements.contains(update.getValue())) {
        return PrepareResponse.newBuilder()
            .setStatus(PrepareResponse.Status.CONCURRENT_TRANSACTION)
            .build();
      }
    }

    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      String element = update.getValue();
      switch (update.getType()) {
        case ADD:
        case NOT_CONTAINS:
          if (set.contains(element)) {
            return PrepareResponse.newBuilder()
                .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
                .build();
          }
          break;
        case REMOVE:
        case CONTAINS:
          if (!set.contains(element)) {
            return PrepareResponse.newBuilder()
                .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
                .build();
          }
          break;
      }
    }

    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      lockedElements.add(update.getValue());
    }
    transactions.put(request.getTransactionId(), request.getTransaction());
    return PrepareResponse.newBuilder()
        .setStatus(PrepareResponse.Status.OK)
        .build();
  }

  @Override
  public PrepareResponse prepareAndCommit(PrepareRequest request) {
    PrepareResponse response = prepare(request);
    if (response.getStatus() == PrepareResponse.Status.OK) {
      commit(CommitRequest.newBuilder()
          .setTransactionId(request.getTransactionId())
          .build());
    }
    return response;
  }

  @Override
  public CommitResponse commit(CommitRequest request) {
    DistributedSetTransaction transaction = transactions.remove(request.getTransactionId());
    if (transaction == null) {
      return CommitResponse.newBuilder()
          .setStatus(CommitResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    }

    for (DistributedSetUpdate update : transaction.getUpdatesList()) {
      switch (update.getType()) {
        case ADD:
          set.add(update.getValue());
          break;
        case REMOVE:
          set.remove(update.getValue());
          break;
        default:
          break;
      }
      lockedElements.remove(update.getValue());
    }
    return CommitResponse.newBuilder()
        .setStatus(CommitResponse.Status.OK)
        .build();
  }

  @Override
  public RollbackResponse rollback(RollbackRequest request) {
    DistributedSetTransaction transaction = transactions.remove(request.getTransactionId());
    if (transaction == null) {
      return RollbackResponse.newBuilder()
          .setStatus(RollbackResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    }

    for (DistributedSetUpdate update : transaction.getUpdatesList()) {
      lockedElements.remove(update.getValue());
    }
    return RollbackResponse.newBuilder()
        .setStatus(RollbackResponse.Status.OK)
        .build();
  }

  private void onEvent(ListenResponse event) {
    getSessions()
        .forEach(session -> session.getStreams(SetOperations.LISTEN_STREAM)
            .forEach(stream -> stream.next(event)));
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    DistributedSetSnapshot.newBuilder()
        .addAllValues(set)
        .addAllLockedElements(lockedElements)
        .putAllTransactions(transactions)
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    DistributedSetSnapshot snapshot = DistributedSetSnapshot.parseFrom(input);
    set = Sets.newConcurrentHashSet(snapshot.getValuesList());
    lockedElements = new CopyOnWriteArraySet<>(snapshot.getLockedElementsList());
    transactions = new HashMap<>(snapshot.getTransactionsMap());
  }
}