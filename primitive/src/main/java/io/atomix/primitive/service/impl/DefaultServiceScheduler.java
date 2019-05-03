package io.atomix.primitive.service.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;

import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.Scheduler;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Service scheduler.
 */
public class DefaultServiceScheduler implements Scheduler, Executor {
  private final PrimitiveService.Context context;
  private final Logger log;
  private final Queue<Runnable> tasks = new LinkedList<>();
  private final List<ScheduledTask> scheduledTasks = new ArrayList<>();
  private final List<ScheduledTask> complete = new ArrayList<>();
  private long timestamp;

  public DefaultServiceScheduler(PrimitiveService.Context context) {
    this.context = context;
    this.log = context.getLogger();
  }

  @Override
  public void execute(Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkNotNull(callback, "callback cannot be null");
    tasks.add(callback);
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkArgument(!delay.isNegative(), "delay cannot be negative");
    checkNotNull(callback, "callback cannot be null");
    log.trace("Scheduled callback {} with delay {}", callback, delay);
    return new ScheduledTask(callback, delay.toMillis()).schedule();
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkArgument(!initialDelay.isNegative(), "initialDelay cannot be negative");
    checkArgument(!interval.isNegative(), "interval cannot be negative");
    checkNotNull(callback, "callback cannot be null");
    log.trace("Scheduled repeating callback {} with initial delay {} and interval {}", callback, initialDelay, interval);
    return new ScheduledTask(callback, initialDelay.toMillis(), interval.toMillis()).schedule();
  }

  /**
   * Checks that the current operation is of the given type.
   *
   * @param type    the operation type
   * @param message the message to print if the current operation does not match the given type
   */
  private void checkOperation(OperationType type, String message) {
    checkState(context.getOperationType() == type, message);
  }

  /**
   * Runs tasks scheduled prior to the given timestamp.
   *
   * @param timestamp the timestamp to which to advance the executor
   */
  public void runScheduledTasks(long timestamp) {
    this.timestamp = timestamp;
    if (!scheduledTasks.isEmpty()) {
      // Iterate through scheduled tasks until we reach a task that has not met its scheduled time.
      // The tasks list is sorted by time on insertion.
      Iterator<ScheduledTask> iterator = scheduledTasks.iterator();
      while (iterator.hasNext()) {
        ScheduledTask task = iterator.next();
        if (task.isRunnable(timestamp)) {
          this.timestamp = task.time;
          log.trace("Executing scheduled task {}", task);
          task.execute();
          complete.add(task);
          iterator.remove();
        } else {
          break;
        }
      }

      // Iterate through tasks that were completed and reschedule them.
      for (ScheduledTask task : complete) {
        task.reschedule(this.timestamp);
      }
      complete.clear();
    }
  }

  /**
   * Executes pending tasks.
   */
  public void runPendingTasks() {
    // Execute any tasks that were queue during execution of the command.
    if (!tasks.isEmpty()) {
      for (Runnable task : tasks) {
        log.trace("Executing task {}", task);
        task.run();
      }
      tasks.clear();
    }
  }

  /**
   * Scheduled task.
   */
  private class ScheduledTask implements Scheduled {
    private final long interval;
    private final Runnable callback;
    private long time;

    private ScheduledTask(Runnable callback, long delay) {
      this(callback, delay, 0);
    }

    private ScheduledTask(Runnable callback, long delay, long interval) {
      this.interval = interval;
      this.callback = callback;
      this.time = timestamp + delay;
    }

    /**
     * Schedules the task.
     */
    private Scheduled schedule() {
      // Perform binary search to insert the task at the appropriate position in the tasks list.
      if (scheduledTasks.isEmpty()) {
        scheduledTasks.add(this);
      } else {
        int l = 0;
        int u = scheduledTasks.size() - 1;
        int i;
        while (true) {
          i = (u + l) / 2;
          long t = scheduledTasks.get(i).time;
          if (t == time) {
            scheduledTasks.add(i, this);
            return this;
          } else if (t < time) {
            l = i + 1;
            if (l > u) {
              scheduledTasks.add(i + 1, this);
              return this;
            }
          } else {
            u = i - 1;
            if (l > u) {
              scheduledTasks.add(i, this);
              return this;
            }
          }
        }
      }
      return this;
    }

    /**
     * Reschedules the task.
     */
    private void reschedule(long timestamp) {
      if (interval > 0) {
        time = timestamp + interval;
        schedule();
      }
    }

    /**
     * Returns a boolean value indicating whether the task delay has been met.
     */
    private boolean isRunnable(long timestamp) {
      return timestamp > time;
    }

    /**
     * Executes the task.
     */
    private synchronized void execute() {
      try {
        callback.run();
      } catch (Exception e) {
        log.error("An exception occurred in a scheduled task", e);
      }
    }

    @Override
    public synchronized void cancel() {
      scheduledTasks.remove(this);
    }
  }
}
