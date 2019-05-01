package io.atomix.core.test.protocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.impl.ServiceManagerStateMachine;

/**
 * Test state machine.
 */
public class TestStateMachine extends ServiceManagerStateMachine {
  private static final Map<PartitionId, TestStateMachine> partitions = new ConcurrentHashMap<>();

  /**
   * Returns the test state machine instance.
   *
   * @return the test state machine instance
   */
  public static TestStateMachine getInstance(PartitionId partitionId, PartitionManagementService managementService) {
    return partitions.computeIfAbsent(partitionId, id -> new TestStateMachine(partitionId, managementService));
  }

  /**
   * Destroys all instances of the state machine.
   */
  public static void destroyInstances() {
    partitions.clear();
  }

  private final TestStateMachineContext context;

  private TestStateMachine(PartitionId partitionId, PartitionManagementService managementService) {
    super(partitionId, managementService);
    this.context = new TestStateMachineContext();
    init(context);
  }

  /**
   * Returns the test state machine context.
   *
   * @return the test state machine context
   */
  public TestStateMachineContext context() {
    return context;
  }
}
