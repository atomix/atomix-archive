package io.atomix.core.map.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.atomix.core.impl.Metadata;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduled;

/**
 * Map service.
 */
public class MapService extends AbstractMapService {
  public static final Type TYPE = new Type();

  /**
   * Map service type.
   */
  public static class Type implements ServiceType {
    private static final String NAME = "map";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new MapService(partitionId, managementService);
    }
  }

  private Map<String, AtomicMapEntryValue> map = new ConcurrentHashMap<>();
  private Set<String> preparedKeys = new HashSet<>();
  private Map<String, AtomicMapTransaction> activeTransactions = new HashMap<>();
  private Set<SessionId> listeners = new LinkedHashSet<>();
  private long currentVersion;
  private Map<String, Scheduled> timers = new HashMap<>();

  public MapService(PartitionId partitionId, PartitionManagementService managementService) {
    super(partitionId, managementService);
  }

  @Override
  public SizeResponse size(SizeRequest request) {
    return SizeResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setSize(map.size())
        .build();
  }

  @Override
  public ContainsKeyResponse containsKey(ContainsKeyRequest request) {
    if (request.getKeysCount() == 0) {
      return ContainsKeyResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setContainsKey(false)
          .build();
    } else if (request.getKeysCount() == 1) {
      AtomicMapEntryValue value = map.get(request.getKeys(0));
      return ContainsKeyResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
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
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setContainsKey(containsKeys)
          .build();
    }
  }

  @Override
  public PutResponse put(PutRequest request) {
    if (isLocked(request.getKey())) {
      return PutResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue oldValue = map.get(request.getKey());
    if (oldValue == null || oldValue.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {
      AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
          .setType(AtomicMapEntryValue.Type.VALUE)
          .setValue(request.getValue())
          .setVersion(getCurrentIndex())
          .setTtl(request.getTtl())
          .setCreated(getCurrentTimestamp())
          .build();
      map.put(request.getKey(), newValue);

      scheduleTtl(request.getKey(), newValue);

      onEvent(MapEvent.newBuilder()
          .setType(MapEvent.Type.INSERTED)
          .setKey(request.getKey())
          .setNewValue(newValue.getValue())
          .setNewVersion(newValue.getVersion())
          .build());

      return PutResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.OK)
          .build();
    } else {
      if (oldValue.getValue().equals(request.getValue())) {
        return PutResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setStatus(UpdateStatus.NOOP)
            .setPreviousValue(oldValue.getValue())
            .setPreviousVersion(oldValue.getVersion())
            .build();
      }

      AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
          .setType(AtomicMapEntryValue.Type.VALUE)
          .setValue(request.getValue())
          .setVersion(getCurrentIndex())
          .setTtl(request.getTtl())
          .setCreated(getCurrentTimestamp())
          .build();
      map.put(request.getKey(), newValue);

      scheduleTtl(request.getKey(), newValue);

      onEvent(MapEvent.newBuilder()
          .setType(MapEvent.Type.UPDATED)
          .setKey(request.getKey())
          .setOldValue(oldValue.getValue())
          .setOldVersion(oldValue.getVersion())
          .setNewValue(newValue.getValue())
          .setNewVersion(newValue.getVersion())
          .build());

      return PutResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.OK)
          .setPreviousValue(oldValue.getValue())
          .setPreviousVersion(oldValue.getVersion())
          .build();
    }
  }

  @Override
  public ReplaceResponse replace(ReplaceRequest request) {
    if (isLocked(request.getKey())) {
      return ReplaceResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue oldValue = map.get(request.getKey());
    if (oldValue == null || oldValue.getType() == AtomicMapEntryValue.Type.TOMBSTONE) {
      if (request.getPreviousValue().isEmpty() && request.getPreviousVersion() == 0) {
        AtomicMapEntryValue newValue = AtomicMapEntryValue.newBuilder()
            .setType(AtomicMapEntryValue.Type.VALUE)
            .setValue(request.getNewValue())
            .setVersion(getCurrentIndex())
            .setTtl(request.getTtl())
            .setCreated(getCurrentTimestamp())
            .build();
        map.put(request.getKey(), newValue);

        scheduleTtl(request.getKey(), newValue);

        onEvent(MapEvent.newBuilder()
            .setType(MapEvent.Type.INSERTED)
            .setKey(request.getKey())
            .setNewValue(newValue.getValue())
            .setNewVersion(newValue.getVersion())
            .build());

        return ReplaceResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setStatus(UpdateStatus.OK)
            .build();
      } else {
        return ReplaceResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setStatus(UpdateStatus.PRECONDITION_FAILED)
            .build();
      }
    } else {
      if ((!request.getPreviousValue().isEmpty() && request.getPreviousValue().equals(oldValue.getValue()))
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

        onEvent(MapEvent.newBuilder()
            .setType(MapEvent.Type.UPDATED)
            .setKey(request.getKey())
            .setNewValue(newValue.getValue())
            .setNewVersion(newValue.getVersion())
            .build());

        return ReplaceResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setStatus(UpdateStatus.OK)
            .setPreviousValue(oldValue.getValue())
            .setPreviousVersion(oldValue.getVersion())
            .build();
      } else {
        return ReplaceResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
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
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setValue(value.getValue())
          .setVersion(value.getVersion())
          .build();
    }
    return GetResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .build();
  }

  @Override
  public RemoveResponse remove(RemoveRequest request) {
    if (isLocked(request.getKey())) {
      return RemoveResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.WRITE_LOCK)
          .build();
    }

    AtomicMapEntryValue value = map.get(request.getKey());
    if (value != null && value.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
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
      onEvent(MapEvent.newBuilder()
          .setType(MapEvent.Type.REMOVED)
          .setKey(request.getKey())
          .setOldValue(value.getValue())
          .setOldVersion(value.getVersion())
          .build());

      return RemoveResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setStatus(UpdateStatus.OK)
          .setPreviousValue(value.getValue())
          .setPreviousVersion(value.getVersion())
          .build();
    }
    return RemoveResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setStatus(UpdateStatus.NOOP)
        .build();
  }

  @Override
  public ClearResponse clear(ClearRequest request) {
    map.forEach((key, value) -> onEvent(MapEvent.newBuilder()
        .setType(MapEvent.Type.REMOVED)
        .setKey(key)
        .setOldValue(value.getValue())
        .setOldVersion(value.getVersion())
        .build()));
    map.clear();
    return ClearResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .build();
  }

  @Override
  public ListenResponse listen(ListenRequest request) {
    listeners.add(getCurrentSession().sessionId());
    return ListenResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .build();
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    listeners.remove(getCurrentSession().sessionId());
    return UnlistenResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .build();
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
              .setMetadata(Metadata.newBuilder()
                  .setIndex(getCurrentIndex())
                  .build())
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
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
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
              .setMetadata(Metadata.newBuilder()
                  .setIndex(getCurrentIndex())
                  .build())
              .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
              .build();
        }
      } else {
        // If the value is non-null, compare the current version with the record version.
        if (existingValue.getVersion() > update.getVersion()) {
          return PrepareResponse.newBuilder()
              .setMetadata(Metadata.newBuilder()
                  .setIndex(getCurrentIndex())
                  .build())
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
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
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
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
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
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
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

      MapEvent event;
      if (newValue != null) {
        map.put(key, newValue);
        if (newValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE && previousValue != null && previousValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
          event = MapEvent.newBuilder()
              .setType(MapEvent.Type.UPDATED)
              .setKey(key)
              .setOldValue(previousValue.getValue())
              .setOldVersion(previousValue.getVersion())
              .setNewValue(newValue.getValue())
              .setNewVersion(newValue.getVersion())
              .build();
        } else if (newValue.getType() != AtomicMapEntryValue.Type.TOMBSTONE) {
          event = MapEvent.newBuilder()
              .setType(MapEvent.Type.INSERTED)
              .setKey(key)
              .setNewValue(newValue.getValue())
              .setNewVersion(newValue.getVersion())
              .build();
        } else {
          event = MapEvent.newBuilder()
              .setType(MapEvent.Type.REMOVED)
              .setKey(key)
              .setOldValue(previousValue.getValue())
              .setOldVersion(previousValue.getVersion())
              .build();
        }
      } else {
        event = MapEvent.newBuilder()
            .setType(MapEvent.Type.REMOVED)
            .setKey(key)
            .setOldValue(previousValue.getValue())
            .setOldVersion(previousValue.getVersion())
            .build();
      }
      onEvent(event);
    }
    return CommitResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setStatus(CommitResponse.Status.OK)
        .build();
  }

  @Override
  public RollbackResponse rollback(RollbackRequest request) {
    AtomicMapTransaction transaction = activeTransactions.remove(request.getTransactionId());
    if (transaction == null) {
      return RollbackResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
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
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
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
  protected void onEvent(MapEvent event) {
    listeners.forEach(sessionId -> onEvent(sessionId, event));
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
        onEvent(MapEvent.newBuilder()
            .setType(MapEvent.Type.REMOVED)
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
  protected void onExpire(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  protected void onClose(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicMapSnapshot.newBuilder()
        .addAllListeners(listeners.stream()
            .map(SessionId::id)
            .collect(Collectors.toList()))
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
    listeners = snapshot.getListenersList().stream()
        .map(SessionId::from)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    preparedKeys = new HashSet<>(snapshot.getPreparedKeysList());
    map = new ConcurrentHashMap<>(snapshot.getEntriesMap());
    activeTransactions = new HashMap<>(snapshot.getTransactionsMap());
    currentVersion = snapshot.getVersion();
    map.forEach((key, value) -> scheduleTtl(key, value));
  }
}
