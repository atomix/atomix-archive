package io.atomix.log.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.log.DistributedLogServer;
import io.atomix.log.protocol.DistributedLogCodec;
import io.atomix.log.protocol.LogEntry;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default distributed log server.
 */
public class DefaultDistributedLogServer implements DistributedLogServer {
  final DistributedLogServerContext context;

  public DefaultDistributedLogServer(DistributedLogServerContext context) {
    this.context = checkNotNull(context, "context cannot be null");
  }

  public DistributedLogServerContext context() {
    return context;
  }

  @Override
  public Role getRole() {
    return context.getRole();
  }

  @Override
  public CompletableFuture<DistributedLogServer> start() {
    return context.start().thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return context.isRunning();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return context.stop();
  }

  /**
   * Default distributed log builder.
   */
  public static class Builder extends DistributedLogServer.Builder {
    @Override
    public DistributedLogServer build() {
      Logger log = ContextualLoggerFactory.getLogger(DistributedLogServer.class, LoggerContext.builder(DistributedLogServer.class)
          .addValue(serverId)
          .build());

      // If a ThreadContextFactory was not provided, create one and ensure it's closed when the server is stopped.
      boolean closeOnStop;
      ThreadContextFactory threadContextFactory;
      if (this.threadContextFactory == null) {
        threadContextFactory = threadModel.factory("log-server-" + serverId + "-%d", threadPoolSize, log);
        closeOnStop = true;
      } else {
        threadContextFactory = this.threadContextFactory;
        closeOnStop = false;
      }

      SegmentedJournal<LogEntry> journal = SegmentedJournal.<LogEntry>builder()
          .withName(serverId)
          .withDirectory(directory)
          .withStorageLevel(storageLevel)
          .withCodec(new DistributedLogCodec())
          .withMaxSegmentSize(maxSegmentSize)
          .withMaxEntrySize(maxEntrySize)
          .withIndexDensity(indexDensity)
          .withFlushOnCommit(flushOnCommit)
          .build();

      return new DefaultDistributedLogServer(new DistributedLogServerContext(
          serverId,
          termProvider,
          protocol,
          replicationFactor,
          replicationStrategy,
          journal,
          maxLogSize,
          maxLogAge,
          threadContextFactory,
          closeOnStop));
    }
  }
}
