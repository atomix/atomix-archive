package io.atomix.client;

import io.atomix.client.counter.AtomicCounterBuilder;
import io.atomix.client.counter.AtomicCounterType;
import io.atomix.client.counter.DistributedCounterBuilder;
import io.atomix.client.counter.DistributedCounterType;
import io.atomix.client.election.LeaderElectionBuilder;
import io.atomix.client.election.LeaderElectionType;
import io.atomix.client.idgenerator.AtomicIdGeneratorBuilder;
import io.atomix.client.idgenerator.AtomicIdGeneratorType;
import io.atomix.client.lock.AtomicLockBuilder;
import io.atomix.client.lock.AtomicLockType;
import io.atomix.client.lock.DistributedLockBuilder;
import io.atomix.client.lock.DistributedLockType;
import io.atomix.client.log.DistributedLogBuilder;
import io.atomix.client.log.DistributedLogType;
import io.atomix.client.map.AtomicMapBuilder;
import io.atomix.client.map.AtomicMapType;
import io.atomix.client.map.DistributedMapBuilder;
import io.atomix.client.map.DistributedMapType;
import io.atomix.client.set.DistributedSetBuilder;
import io.atomix.client.set.DistributedSetType;
import io.atomix.client.utils.concurrent.ThreadContextFactory;
import io.atomix.client.value.AtomicValueBuilder;
import io.atomix.client.value.AtomicValueType;
import io.atomix.client.value.DistributedValueBuilder;
import io.atomix.client.value.DistributedValueType;

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
public interface AtomixClientService extends PrimitiveFactory {

  /**
   * Returns the Atomix primitive thread factory.
   *
   * @return the primitive thread context factory
   */
  ThreadContextFactory getThreadFactory();

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
   * Creates a new named {@link io.atomix.client.map.DistributedMap} builder.
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
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a distributed map
   */
  default <K, V> DistributedMapBuilder<K, V> mapBuilder(String name) {
    return primitiveBuilder(name, DistributedMapType.instance());
  }

  /**
   * Creates a new named {@link io.atomix.client.map.AtomicMap} builder.
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
   * @param <K>  key type
   * @param <V>  value type
   * @return builder for a atomic map
   */
  default <K, V> AtomicMapBuilder<K, V> atomicMapBuilder(String name) {
    return primitiveBuilder(name, AtomicMapType.instance());
  }

  /**
   * Creates a new named {@link io.atomix.client.set.DistributedSet} builder.
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
   * @param <E>  set element type
   * @return builder for an distributed set
   */
  default <E> DistributedSetBuilder<E> setBuilder(String name) {
    return primitiveBuilder(name, DistributedSetType.instance());
  }

  /**
   * Creates a new named {@link io.atomix.client.counter.DistributedCounter} builder.
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
   * Creates a new named {@link io.atomix.client.counter.AtomicCounter} builder.
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
   * Creates a new named {@link io.atomix.client.idgenerator.AtomicIdGenerator} builder.
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
   * Creates a new named {@link io.atomix.client.value.DistributedValue} builder.
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
   * @param <V>  atomic value type
   * @return atomic value builder
   */
  default <V> DistributedValueBuilder<V> valueBuilder(String name) {
    return primitiveBuilder(name, DistributedValueType.instance());
  }

  /**
   * Creates a new named {@link io.atomix.client.value.AtomicValue} builder.
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
   * @param <V>  atomic value type
   * @return atomic value builder
   */
  default <V> AtomicValueBuilder<V> atomicValueBuilder(String name) {
    return primitiveBuilder(name, AtomicValueType.instance());
  }

  /**
   * Creates a new named {@link io.atomix.client.lock.DistributedLock} builder.
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
   * Creates a new named {@link io.atomix.client.lock.AtomicLock} builder.
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
   * Creates a new named {@link io.atomix.client.election.LeaderElection} builder.
   * <p>
   * The election name must be provided when constructing the builder. The name is used to reference a distinct instance of
   * the primitive within the cluster. Multiple instances of the primitive with the same name will share the same state.
   * However, the instance of the primitive constructed by the returned builder will be distinct and will not share
   * local memory (e.g. cache) with any other instance on this node.
   * <p>
   * To get an asynchronous instance of the election, use the {@link SyncPrimitive#async()} method:
   * <pre>
   *   {@code
   *   AsyncLeaderElection<String> election = atomix.<String>leaderElectionBuilder("my-election").build().async();
   *   }
   * </pre>
   *
   * @param name the primitive name
   * @return distributed leader election builder
   */
  default <T> LeaderElectionBuilder<T> leaderElectionBuilder(String name) {
    return primitiveBuilder(name, LeaderElectionType.instance());
  }

}
