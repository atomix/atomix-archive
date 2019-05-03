package io.atomix.core;

import io.atomix.cluster.ClusterService;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.counter.DistributedCounterBuilder;
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGeneratorBuilder;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.AtomicLockBuilder;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.log.DistributedLogBuilder;
import io.atomix.core.log.DistributedLogType;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueType;
import io.atomix.core.value.DistributedValue;
import io.atomix.core.value.DistributedValueBuilder;
import io.atomix.core.value.DistributedValueType;
import io.atomix.primitive.PrimitiveFactory;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.utils.Version;
import io.atomix.utils.concurrent.ThreadContextFactory;

/**
 * Manages the creation of distributed primitive instances.
 * <p>
 * The primitives service provides various methods for constructing core and custom distributed primitives.
 * The service provides various methods for creating and operating on distributed primitives. Generally, the primitive
 * methods are separated into two types. Primitive getters return multiton instances of a primitive. Primitives created
 * via getters must be pre-configured in the Atomix instance configuration. Alternatively, primitive builders can be
 * used to create and configure primitives in code:
 * <pre>
 *   {@code
 *   AtomicMap<String, String> map = atomix.mapBuilder("my-map")
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 * Custom primitives can be constructed by providing a custom {@link PrimitiveType} and using the
 * {@link #primitiveBuilder(String, PrimitiveType)} method:
 * <pre>
 *   {@code
 *   MyPrimitive myPrimitive = atomix.primitiveBuilder("my-primitive, MyPrimitiveType.instance())
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 */
public interface AtomixService extends PrimitiveFactory, ClusterService {

  /**
   * Returns the Atomix version.
   *
   * @return the Atomix version
   */
  Version getVersion();

  /**
   * Returns the primitive type registry.
   *
   * @return the primitive type registry
   */
  PrimitiveTypeRegistry getPrimitiveTypes();

  /**
   * Returns the protocol type registry.
   *
   * @return the protocol type registry
   */
  PrimitiveProtocolTypeRegistry getProtocolTypes();

  /**
   * Returns the partition group type registry.
   *
   * @return the partition group type registry
   */
  PartitionGroupTypeRegistry getPartitionGroupTypes();

  /**
   * Returns the Atomix primitive thread factory.
   *
   * @return the primitive thread context factory
   */
  ThreadContextFactory getThreadFactory();

  /**
   * Returns the primitive configuration service.
   * <p>
   * The primitive configuration service provides all pre-defined named primitive configurations.
   *
   * @return the primitive configuration service
   */
  ConfigService getConfigService();

  /**
   * Returns the partition service.
   * <p>
   * The partition service is responsible for managing the lifecycle of primitive partitions and can provide information
   * about active partition groups and partitions in the cluster.
   *
   * @return the partition service
   */
  PartitionService getPartitionService();

  /**
   * Returns the transaction service.
   * <p>
   * The transaction service is responsible for managing the lifecycle of all transactions in the cluster and can
   * provide information about currently active transactions.
   *
   * @return the transaction service
   */
  TransactionService getTransactionService();

  /**
   * Returns the session ID service.
   * <p>
   * The session ID service is responsible for generating globally unique session identifiers.
   *
   * @return the session ID service
   */
  SessionIdService getSessionIdService();

  /**
   * Creates a new log primitive builder.
   *
   * @param <E> the log entry type
   * @return the log builder
   */
  default <E> DistributedLogBuilder<E> logBuilder() {
    return primitiveBuilder("log", DistributedLogType.instance());
  }

  /**
   * Creates a new named {@link DistributedMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedMap<String, String> map = atomix.<String, String>mapBuilder("my-map").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a distributed map
   */
  default <K, V> DistributedMapBuilder<K, V> mapBuilder(String name) {
    return primitiveBuilder(name, DistributedMapType.instance());
  }

  /**
   * Creates a new named {@link AtomicMap} builder.
   * <p>
   * The map name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the map, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicMap<String, String> map = atomix.<String, String>atomicMapBuilder("my-map").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <K> key type
   * @param <V> value type
   * @return builder for a atomic map
   */
  default <K, V> AtomicMapBuilder<K, V> atomicMapBuilder(String name) {
    return primitiveBuilder(name, AtomicMapType.instance());
  }

  /**
   * Creates a new named {@link DistributedSet} builder.
   * <p>
   * The set name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the set, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedSet<String> set = atomix.<String>setBuilder("my-set").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <E> set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, DistributedSetType.instance());
  }

  /**
   * Creates a new named {@link DistributedCounter} builder.
   * <p>
   * The counter name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedCounter counter = atomix.counterBuilder("my-counter").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed counter builder
   */
  default DistributedCounterBuilder counterBuilder(String name) {
    return primitiveBuilder(name, DistributedCounterType.instance());
  }

  /**
   * Creates a new named {@link AtomicCounter} builder.
   * <p>
   * The counter name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the counter, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicCounter counter = atomix.atomicCounterBuilder("my-counter").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic counter builder
   */
  default AtomicCounterBuilder atomicCounterBuilder(String name) {
    return primitiveBuilder(name, AtomicCounterType.instance());
  }

  /**
   * Creates a new named {@link AtomicIdGenerator} builder.
   * <p>
   * The ID generator name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the ID generator, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicIdGenerator idGenerator = atomix.atomicIdGeneratorBuilder("my-id-generator").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return atomic ID generator builder
   */
  default AtomicIdGeneratorBuilder atomicIdGeneratorBuilder(String name) {
    return primitiveBuilder(name, AtomicIdGeneratorType.instance());
  }

  /**
   * Creates a new named {@link DistributedValue} builder.
   * <p>
   * The value name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedValue<String> value = atomix.<String>valueBuilder("my-value").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> DistributedValueBuilder<V> valueBuilder(String name) {
    return primitiveBuilder(name, DistributedValueType.instance());
  }

  /**
   * Creates a new named {@link AtomicValue} builder.
   * <p>
   * The value name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the value, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicValue<String> value = atomix.<String>atomicValueBuilder("my-value").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @param <V> atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, AtomicValueType.instance());
  }

  /**
   * Creates a new named {@link DistributedLock} builder.
   * <p>
   * The lock name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncDistributedLock lock = atomix.lockBuilder("my-lock").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default DistributedLockBuilder lockBuilder(String name) {
    return primitiveBuilder(name, DistributedLockType.instance());
  }

  /**
   * Creates a new named {@link AtomicLock} builder.
   * <p>
   * The lock name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the lock, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncAtomicLock lock = atomix.atomicLockBuilder("my-lock").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed lock builder
   */
  default AtomicLockBuilder atomicLockBuilder(String name) {
    return primitiveBuilder(name, AtomicLockType.instance());
  }

  /**
   * Creates a new transaction builder.
   * <p>
   * To get an asynchronous instance of the transaction, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncTransaction transaction = atomix.transactionBuilder().build().async();
   *   }
   * </pre>
   *
   * @return transaction builder
   */
  default TransactionBuilder transactionBuilder() {
    return transactionBuilder("transaction");
  }

  /**
   * Creates a new transaction builder.
   * <p>
   * To get an asynchronous instance of the transaction, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncTransaction transaction = atomix.transactionBuilder().build().async();
   *   }
   * </pre>
   *
   * @param name the transaction name
   * @return the transaction builder
   */
  TransactionBuilder transactionBuilder(String name);

}
