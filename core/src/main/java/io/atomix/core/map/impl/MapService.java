package io.atomix.core.map.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.component.Component;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.stream.StreamHandler;

/**
 * Map service.
 */
public class MapService extends AbstractMapService {
  public static final Type TYPE = new Type();

  static final int VERSION_EMPTY = -1;

  /**
   * Map service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "map";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new MapService();
    }
  }

  private Map<String, AtomicMapEntryValue> map = new ConcurrentHashMap<>();
  private Set<String> preparedKeys = new HashSet<>();
  private Map<String, AtomicMapTransaction> activeTransactions = new HashMap<>();
  private long currentVersion;
  private Map<String, Scheduled> timers = new HashMap<>();

  @Override
  public SizeResponse size(SizeRequest request) {
    return SizeResponse.newBuilder()
        .setSize(map.size())
        .build();
  }

  @Override
  public ContainsKeyResponse containsKey(ContainsKeyRequest request) {
    if (request.getKeysCount() == 0) {
      return ContainsKeyResponse.newBuilder()
          .setContainsKey(false)
          .build();
    } else if (request.getKeysCount() == 1) {
      AtomicMapEntryValue value = map.get(request.getKeys(0));
      return ContainsKeyResponse.newBuilder()
          .setContainsKey(value != null && value.getType() != AtomicMapEntryValue.Type.TOMBSTONE)
          .build();
    } else {
      boolean containsKeys = true;
      for (String key : request.getKeysList()) {
        AtomicMapEntryValue value = map.get(key);
        if (value == null || value.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {
          containsKeys = false;
          break;
        }
      }
      return ContainsKeyResponse.newBuilder()
          .setContainsKey(containsKeys)
          .build();
    }
  }

  @Override
  public PutResponse put(PutRequest request) {
    // If the key is locked, reject the request with a WRITE_LOCK error.
    if (isLocked(request.getKey())) {
      return PutResponse.newBuilder()
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue oldValue = map.get(request.getKey());
    if (oldValue == null || oldValue.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {

      // If the version is positive then reject the request.
      if (request.getVersion() > 0) {
        return PutResponse.newBuilder()
            .setStatus(UpdateStatus.PRECONDITION_FAILED)
            .build();
      }

      // Create a new entry value and set it in the map.
      AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
          .setType(AtomicMapEntryValue.Type.VALUE)
          .setValue(request.getValue())
          .setVersion(getCurrentIndex())
          .setTtl(request.getTtl())
          .setCreated(getCurrentTimestamp())
          .build();
      map.put(request.getKey(), newValue);

      // Schedule the timeout for the value if necessary.
      scheduleTtl(request.getKey(), newValue);

      // Publish an event to listener streams.
      onEvent(ListenResponse.newBuilder()
          .setType(ListenResponse.Type.INSERTED)
          .setKey(request.getKey())
          .setNewValue(newValue.getValue())
          .setNewVersion(newValue.getVersion())
          .build());

      return PutResponse.newBuilder()
          .setStatus(UpdateStatus.OK)
          .build();
    } else {
      // If the version is -1 then reject the request.
      // If the version is positive then compare the version to the current version.
      if (request.getVersion() == VERSION_EMPTY
          || (request.getVersion() > 0 && request.getVersion() != oldValue.getVersion())) {
        return PutResponse.newBuilder()
            .setStatus(UpdateStatus.PRECONDITION_FAILED)
            .setPreviousValue(oldValue.getValue())
            .setPreviousVersion(oldValue.getVersion())
            .build();
      }
    }

    // If the value is equal to the current value, return a no-op.
    if (oldValue.getValue().equals(request.getValue())) {
      return PutResponse.newBuilder()
          .setStatus(UpdateStatus.NOOP)
          .setPreviousValue(oldValue.getValue())
          .setPreviousVersion(oldValue.getVersion())
          .build();
    }

    // Create a new entry value and set it in the map.
    AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
        .setType(AtomicMapEntryValue.Type.VALUE)
        .setValue(request.getValue())
        .setVersion(getCurrentIndex())
        .setTtl(request.getTtl())
        .setCreated(getCurrentTimestamp())
        .build();
    map.put(request.getKey(), newValue);

    // Schedule the timeout for the value if necessary.
    scheduleTtl(request.getKey(), newValue);

    // Publish an event to listener streams.
    onEvent(ListenResponse.newBuilder()
        .setType(ListenResponse.Type.UPDATED)
        .setKey(request.getKey())
        .setOldValue(oldValue.getValue())
        .setOldVersion(oldValue.getVersion())
        .setNewValue(newValue.getValue())
        .setNewVersion(newValue.getVersion())
        .build());

    return PutResponse.newBuilder()
        .setStatus(UpdateStatus.OK)
        .setPreviousValue(oldValue.getValue())
        .setPreviousVersion(oldValue.getVersion())
        .build();
  }

  @Override
  public ReplaceResponse replace(ReplaceRequest request) {
    // If the key is locked, reject the request with a WRITE_LOCK error.
    if (isLocked(request.getKey())) {
      return ReplaceResponse.newBuilder()
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue oldValue = map.get(request.getKey());
    if (oldValue == null || oldValue.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {
      return ReplaceResponse.newBuilder()
          .setStatus(UpdateStatus.PRECONDITION_FAILED)
          .build();
    } else {
      if ((request.getPreviousValue().isEmpty() && request.getPreviousVersion() == 0)
          || (!request.getPreviousValue().isEmpty() && request.getPreviousValue().equals(oldValue.getValue()))
          || (request.getPreviousVersion() != 0 && request.getPreviousVersion() == oldValue.getVersion())) {
        AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
            .setType(AtomicMapEntryValue.Type.VALUE)
            .setValue(request.getNewValue())
            .setVersion(getCurrentIndex())
            .setTtl(request.getTtl())
            .setCreated(getCurrentTimestamp())
            .build();
        map.put(request.getKey(), newValue);

        scheduleTtl(request.getKey(), newValue);

        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.UPDATED)
            .setKey(request.getKey())
            .setNewValue(newValue.getValue())
            .setNewVersion(newValue.getVersion())
            .build());

        return ReplaceResponse.newBuilder()
            .setStatus(UpdateStatus.OK)
            .setPreviousValue(oldValue.getValue())
            .setPreviousVersion(oldValue.getVersion())
            .build();
      } else {
        return ReplaceResponse.newBuilder()
            .setStatus(UpdateStatus.PRECONDITION_FAILED)
            .build();
      }
    }
  }

  @Override
  public GetResponse get(GetRequest request) {
    AtomicMapEntryValue value = map.get(request.getKey());
    if (value != null && value.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
      return GetResponse.newBuilder()
          .setValue(value.getValue())
          .setVersion(value.getVersion())
          .build();
    }
    return GetResponse.newBuilder().build();
  }

  @Override
  public RemoveResponse remove(RemoveRequest request) {
    // If the key is locked, reject the request with a WRITE_LOCK error.
    if (isLocked(request.getKey())) {
      return RemoveResponse.newBuilder()
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue value = map.get(request.getKey());
    if (value != null && value.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
      // If the request version is set, compare the version to the current entry version.
      if (request.getVersion() > 0 && request.getVersion() != value.getVersion()) {
        return RemoveResponse.newBuilder()
            .setStatus(UpdateStatus.PRECONDITION_FAILED)
            .build();
      }

      // If no transactions are active, remove the value from the map. Otherwise, update its type to TOMBSTONE.
      if (activeTransactions.isEmpty()) {
        map.remove(request.getKey());
      } else {
        map.put(request.getKey(), AtomicMapEntryValue.newBuilder(value)
            .setType(AtomicMapEntryValue.Type.TOMBSTONE)
            .build());
      }

      // Cancel the TTL.
      cancelTtl(request.getKey());

      // Trigger a REMOVED event.
      onEvent(ListenResponse.newBuilder()
          .setType(ListenResponse.Type.REMOVED)
          .setKey(request.getKey())
          .setOldValue(value.getValue())
          .setOldVersion(value.getVersion())
          .build());

      return RemoveResponse.newBuilder()
          .setStatus(UpdateStatus.OK)
          .setPreviousValue(value.getValue())
          .setPreviousVersion(value.getVersion())
          .build();
    }
    return RemoveResponse.newBuilder()
        .setStatus(UpdateStatus.NOOP)
        .build();
  }

  @Override
  public ClearResponse clear(ClearRequest request) {
    map.forEach((key, value) -> onEvent(ListenResponse.newBuilder()
        .setType(ListenResponse.Type.REMOVED)
        .setKey(key)
        .setOldValue(value.getValue())
        .setOldVersion(value.getVersion())
        .build()));
    map.clear();
    return ClearResponse.newBuilder().build();
  }

  @Override
  public void keys(KeysRequest request, StreamHandler<KeysResponse> handler) {
    map.forEach((key, value) -> {
      if (value.getType() == AtomicMapEntryValue.Type.VALUE) {
        handler.next(KeysResponse.newBuilder()
            .setKey(key)
            .build());
      }
    });
    handler.complete();
  }

  @Override
  public void entries(EntriesRequest request, StreamHandler<EntriesResponse> handler) {
    map.forEach((key, value) -> {
      if (value.getType() == AtomicMapEntryValue.Type.VALUE) {
        handler.next(EntriesResponse.newBuilder()
            .setKey(key)
            .setValue(value.getValue())
            .setVersion(value.getVersion())
            .build());
      }
    });
    handler.complete();
  }

  @Override
  public void listen(ListenRequest request, StreamHandler<ListenResponse> handler) {
    // Keep the stream open.
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    // Close the stream.
    StreamHandler<ListenRequest> stream = getCurrentSession().getStream(request.getStreamId());
    if (stream != null) {
      stream.complete();
    }
    return UnlistenResponse.newBuilder().build();
  }

  @Override
  public PrepareResponse prepare(PrepareRequest request) {
    // Iterate through records in the transaction log and perform isolation checks.
    for (AtomicMapUpdate update : request.getTransaction().getUpdatesList()) {
      String key = update.getKey();

      // If the record is a VERSION_MATCH then check that the record's version matches the current
      // version of the state machine.
      if (update.getType() == AtomicMapUpdate.Type.VERSION_MATCH && key == null) {
        if (update.getVersion() > currentVersion) {
          return PrepareResponse.newBuilder()
              .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
              .build();
        } else {
          continue;
        }
      }

      // If the prepared keys already contains the key contained within the record, that indicates a
      // conflict with a concurrent transaction.
      if (preparedKeys.contains(key)) {
        return PrepareResponse.newBuilder()
            .setStatus(PrepareResponse.Status.CONCURRENT_TRANSACTION)
            .build();
      }

      // Read the existing value from the map.
      AtomicMapEntryValue existingValue = map.get(key);

      // Note: if the existing value is null, that means the key has not changed during the transaction,
      // otherwise a tombstone would have been retained.
      if (existingValue == null) {
        // If the value is null, ensure the version is equal to the transaction version.
        if (update.getType() != AtomicMapUpdate.Type.PUT_IF_ABSENT && update.getVersion() != request.getTransaction().getVersion()) {
          return PrepareResponse.newBuilder()
              .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
              .build();
        }
      } else {
        // If the value is non-null, compare the current version with the record version.
        if (existingValue.getVersion() > update.getVersion()) {
          return PrepareResponse.newBuilder()
              .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
              .build();
        }
      }
    }

    // No violations detected. Mark modified keys locked for transactions.
    request.getTransaction().getUpdatesList().forEach(update -> {
      if (update.getType() != AtomicMapUpdate.Type.VERSION_MATCH) {
        preparedKeys.add(update.getKey());
      }
    });

    activeTransactions.put(request.getTransactionId(), request.getTransaction());
    return PrepareResponse.newBuilder()
        .setStatus(PrepareResponse.Status.OK)
        .build();
  }

  @Override
  public PrepareResponse prepareAndCommit(PrepareRequest request) {
    String transactionId = request.getTransactionId();
    PrepareResponse prepareResponse = prepare(request);
    AtomicMapTransaction transaction = activeTransactions.remove(transactionId);
    if (prepareResponse.getStatus() == PrepareResponse.Status.OK) {
      this.currentVersion = getCurrentIndex();
      commitTransaction(transaction);
    }
    discardTombstones();
    return prepareResponse;
  }

  @Override
  public CommitResponse commit(CommitRequest request) {
    AtomicMapTransaction transaction = activeTransactions.remove(request.getTransactionId());
    if (transaction == null) {
      return CommitResponse.newBuilder()
          .setStatus(CommitResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    }

    try {
      this.currentVersion = getCurrentIndex();
      return commitTransaction(transaction);
    } finally {
      discardTombstones();
    }
  }

  /**
   * Applies committed operations to the state machine.
   */
  private CommitResponse commitTransaction(AtomicMapTransaction transaction) {
    boolean retainTombstones = !activeTransactions.isEmpty();

    for (AtomicMapUpdate update : transaction.getUpdatesList()) {
      if (update.getType() == AtomicMapUpdate.Type.VERSION_MATCH) {
        continue;
      }

      String key = update.getKey();
      if (!preparedKeys.remove(key)) {
        return CommitResponse.newBuilder()
            .setStatus(CommitResponse.Status.FAILURE_DURING_COMMIT)
            .build();
      }

      if (update.getType() == AtomicMapUpdate.Type.LOCK) {
        continue;
      }

      AtomicMapEntryValue previousValue = map.remove(key);

      // Cancel the previous timer if set.
      cancelTtl(key);

      AtomicMapEntryValue newValue = null;

      // If the record is not a delete, create a transactional commit.
      if (update.getType() != AtomicMapUpdate.Type.REMOVE_IF_VERSION_MATCH) {
        newValue = AtomicMapEntryValue.newBuilder()
            .setType(AtomicMapEntryValue.Type.VALUE)
            .setValue(update.getValue())
            .setVersion(currentVersion)
            .build();
      } else if (retainTombstones) {
        // For deletes, if tombstones need to be retained then create and store a tombstone commit.
        newValue = AtomicMapEntryValue.newBuilder()
            .setType(AtomicMapEntryValue.Type.TOMBSTONE)
            .setVersion(currentVersion)
            .build();
      }

      ListenResponse event;
      if (newValue != null) {
        map.put(key, newValue);
        if (newValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE && previousValue != null && previousValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
          event = ListenResponse.newBuilder()
              .setType(ListenResponse.Type.UPDATED)
              .setKey(key)
              .setOldValue(previousValue.getValue())
              .setOldVersion(previousValue.getVersion())
              .setNewValue(newValue.getValue())
              .setNewVersion(newValue.getVersion())
              .build();
        } else if (newValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
          event = ListenResponse.newBuilder()
              .setType(ListenResponse.Type.INSERTED)
              .setKey(key)
              .setNewValue(newValue.getValue())
              .setNewVersion(newValue.getVersion())
              .build();
        } else {
          event = ListenResponse.newBuilder()
              .setType(ListenResponse.Type.REMOVED)
              .setKey(key)
              .setOldValue(previousValue.getValue())
              .setOldVersion(previousValue.getVersion())
              .build();
        }
      } else {
        event = ListenResponse.newBuilder()
            .setType(ListenResponse.Type.REMOVED)
            .setKey(key)
            .setOldValue(previousValue.getValue())
            .setOldVersion(previousValue.getVersion())
            .build();
      }
      onEvent(event);
    }
    return CommitResponse.newBuilder()
        .setStatus(CommitResponse.Status.OK)
        .build();
  }

  @Override
  public RollbackResponse rollback(RollbackRequest request) {
    AtomicMapTransaction transaction = activeTransactions.remove(request.getTransactionId());
    if (transaction == null) {
      return RollbackResponse.newBuilder()
          .setStatus(RollbackResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    } else {
      transaction.getUpdatesList().forEach(update -> {
        if (update.getType() != AtomicMapUpdate.Type.VERSION_MATCH) {
          preparedKeys.remove(update.getKey());
        }
      });
      discardTombstones();
      return RollbackResponse.newBuilder()
          .setStatus(RollbackResponse.Status.OK)
          .build();
    }
  }

  /**
   * Discards tombstones no longer needed by active transactions.
   */
  private void discardTombstones() {
    if (activeTransactions.isEmpty()) {
      Iterator<Map.Entry<String, AtomicMapEntryValue>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        AtomicMapEntryValue value = iterator.next().getValue();
        if (value.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {
          iterator.remove();
        }
      }
    } else {
      long minimumVersion = activeTransactions.values().stream()
          .mapToLong(AtomicMapTransaction::getVersion)
          .min()
          .getAsLong();
      Iterator<Map.Entry<String, AtomicMapEntryValue>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        AtomicMapEntryValue value = iterator.next().getValue();
        if (value.getType() == AtomicMapEntryValue.Type.TOMBSTONE && value.getVersion() < minimumVersion) {
          iterator.remove();
        }
      }
    }
  }

  /**
   * Returns a boolean indicating whether the given key is locked.
   *
   * @param key the key to check
   * @return indicates whether the key is locked by a running transaction
   */
  private boolean isLocked(String key) {
    return preparedKeys.contains(key);
  }

  /**
   * Publishes a map event to all registered sessions.
   *
   * @param event the event to publish
   */
  protected void onEvent(ListenResponse event) {
    getSessions()
        .forEach(session -> session.getStreams(MapOperations.LISTEN_STREAM)
            .forEach(stream -> stream.next(event)));
  }

  /**
   * Schedules the TTL for the given value.
   *
   * @param value the value for which to schedule the TTL
   */
  protected void scheduleTtl(String key, AtomicMapEntryValue value) {
    cancelTtl(key);
    if (value.getTtl() > 0) {
      timers.put(key, getScheduler().schedule(Duration.ofMillis(value.getTtl() - (getCurrentTimestamp() - value.getCreated())), () -> {
        map.remove(key, value);
        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.REMOVED)
            .setKey(key)
            .setOldValue(value.getValue())
            .setOldVersion(value.getVersion())
            .build());
      }));
    }
  }

  /**
   * Cancels the TTL for the given value.
   *
   * @param key the key for which to cancel the TTL
   */
  protected void cancelTtl(String key) {
    Scheduled timer = timers.remove(key);
    if (timer != null) {
      timer.cancel();
    }
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicMapSnapshot.newBuilder()
        .addAllPreparedKeys(preparedKeys)
        .putAllEntries(map)
        .putAllTransactions(activeTransactions)
        .setVersion(currentVersion)
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    AtomicMapSnapshot snapshot = AtomicMapSnapshot.parseFrom(input);
    preparedKeys = new HashSet<>(snapshot.getPreparedKeysList());
    map = new ConcurrentHashMap<>(snapshot.getEntriesMap());
    activeTransactions = new HashMap<>(snapshot.getTransactionsMap());
    currentVersion = snapshot.getVersion();
    map.forEach((key, value) -> scheduleTtl(key, value));
  }
}
