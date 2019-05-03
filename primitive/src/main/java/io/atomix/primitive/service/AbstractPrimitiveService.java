package io.atomix.primitive.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executor;

import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.concurrent.Scheduler;
import org.slf4j.Logger;

/**
 * Abstract primitive service.
 */
public abstract class AbstractPrimitiveService implements PrimitiveService {
  private Context context;
  private Scheduler scheduler;
  private Executor executor;

  /**
   * Initializes the service.
   *
   * @param context the state machine context
   * @param scheduler the state machine scheduler
   */
  protected void init(Context context, Scheduler scheduler, Executor executor) {
    this.context = context;
    this.scheduler = scheduler;
    this.executor = executor;
  }

  /**
   * Configures the service operations.
   *
   * @param registry the service operation registry
   */
  protected abstract void configure(ServiceOperationRegistry registry);

  /**
   * Returns the current index.
   *
   * @return the current index
   */
  protected long getCurrentIndex() {
    return context.getIndex();
  }

  /**
   * Returns the current timestamp.
   *
   * @return the current timestamp
   */
  protected long getCurrentTimestamp() {
    return context.getTimestamp();
  }

  /**
   * Returns the deterministic service scheduler.
   *
   * @return the deterministic service scheduler
   */
  protected Scheduler getScheduler() {
    return scheduler;
  }

  /**
   * Returns the deterministic service executor.
   *
   * @return the deterministic service executor
   */
  protected Executor getExecutor() {
    return executor;
  }

  /**
   * Returns the service logger.
   *
   * @return the service logger
   */
  protected Logger getLogger() {
    return context.getLogger();
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    backup(output);
  }

  @Override
  public void install(InputStream input) throws IOException {
    restore(input);
  }

  /**
   * Backs up the state of the service.
   *
   * @param output the output to which to backup the service
   */
  protected abstract void backup(OutputStream output) throws IOException;

  /**
   * Restores the state of the service.
   *
   * @param input the input from which to restore the service
   */
  protected abstract void restore(InputStream input) throws IOException;


  /**
   * Session managed service context.
   */
  class Context implements PrimitiveService.Context {
    private final StateMachine.Context parent;
    private OperationType operationType;

    Context(StateMachine.Context parent) {
      this.parent = parent;
    }

    void setOperationType(OperationType operationType) {
      this.operationType = operationType;
    }

    @Override
    public long getIndex() {
      return parent.getIndex();
    }

    @Override
    public long getTimestamp() {
      return parent.getTimestamp();
    }

    @Override
    public OperationType getOperationType() {
      return operationType;
    }

    @Override
    public Logger getLogger() {
      return parent.getLogger();
    }
  }
}
