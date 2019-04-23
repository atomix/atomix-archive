package io.atomix.primitive.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executor;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.utils.concurrent.Scheduler;
import org.slf4j.Logger;

/**
 * Abstract primitive service.
 */
public abstract class AbstractPrimitiveService implements PrimitiveService {
  private Context context;
  private ServiceExecutor executor;

  @Override
  public void init(Context context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context.getLogger());
    configure(executor);
  }

  /**
   * Configures the service operations.
   *
   * @param executor the service executor
   */
  protected abstract void configure(ServiceExecutor executor);

  /**
   * Applies the given command.
   *
   * @param operationId the command operation ID
   * @param command     the command to apply
   * @return the command result
   */
  protected byte[] apply(OperationId operationId, Command<byte[]> command) {
    return executor.apply(operationId, command);
  }

  /**
   * Applies the given query.
   *
   * @param operationId the query operation ID
   * @param query       the query to apply
   * @return the query result
   */
  protected byte[] apply(OperationId operationId, Query<byte[]> query) {
    return executor.apply(operationId, query);
  }

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
    return executor;
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

}
