package io.atomix.log.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import io.atomix.log.DistributedLogClient;
import io.atomix.log.DistributedLogConsumer;
import io.atomix.log.DistributedLogException;
import io.atomix.log.DistributedLogProducer;
import io.atomix.log.Term;
import io.atomix.log.TermProvider;
import io.atomix.log.protocol.AppendRequest;
import io.atomix.log.protocol.ConsumeRequest;
import io.atomix.log.protocol.LogClientProtocol;
import io.atomix.log.protocol.LogRecord;
import io.atomix.log.protocol.RecordsRequest;
import io.atomix.log.protocol.ResetRequest;
import io.atomix.log.protocol.ResponseStatus;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

/**
 * Default distributed log client.
 */
public class DefaultDistributedLogClient implements DistributedLogClient {
  private static final AtomicLong CONSUMER_ID = new AtomicLong();
  private final String clientId;
  private final long consumerId;
  private final LogClientProtocol protocol;
  private final TermProvider termProvider;
  private final ThreadContext threadContext;
  private final ThreadContextFactory threadContextFactory;
  private final boolean closeOnStop;
  private final Consumer<Term> termListener = this::changeTerm;
  private Term term;
  private final DefaultDistributedLogProducer producer = new DefaultDistributedLogProducer();
  private final DefaultDistributedLogConsumer consumer = new DefaultDistributedLogConsumer();

  public DefaultDistributedLogClient(
      String clientId,
      LogClientProtocol protocol,
      TermProvider termProvider,
      ThreadContext threadContext,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.clientId = clientId;
    this.protocol = protocol;
    this.termProvider = termProvider;
    this.threadContext = threadContext;
    this.threadContextFactory = threadContextFactory;
    this.closeOnStop = closeOnStop;
    this.consumerId = CONSUMER_ID.incrementAndGet();
  }

  @Override
  public DistributedLogProducer producer() {
    return producer;
  }

  @Override
  public DistributedLogConsumer consumer() {
    return consumer;
  }

  @Override
  public CompletableFuture<DistributedLogClient> connect() {
    termProvider.addListener(termListener);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    termProvider.removeListener(termListener);
    threadContext.close();
    if (closeOnStop) {
      threadContextFactory.close();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a term change.
   */
  private void changeTerm(Term term) {
    threadContext.execute(() -> {
      if (this.term == null || term.term() > this.term.term()) {
        this.term = term;
        consumer.register(term.leader());
      }
    });
  }

  /**
   * Returns the current primary term.
   *
   * @return the current primary term
   */
  private CompletableFuture<Term> term() {
    CompletableFuture<Term> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (term != null) {
        future.complete(term);
      } else {
        termProvider.getTerm().whenCompleteAsync((term, error) -> {
          if (term != null) {
            this.term = term;
            future.complete(term);
          } else {
            future.completeExceptionally(new DistributedLogException.Unavailable());
          }
        });
      }
    });
    return future;
  }

  /**
   * Distributed log producer.
   */
  private class DefaultDistributedLogProducer implements DistributedLogProducer {
    @Override
    public CompletableFuture<Long> append(byte[] value) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      term().thenCompose(term -> protocol.append(term.leader(), AppendRequest.newBuilder()
          .setValue(ByteString.copyFrom(value))
          .build()))
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              if (response.getStatus() == ResponseStatus.OK) {
                future.complete(response.getIndex());
              } else {
                future.completeExceptionally(new DistributedLogException.Unavailable());
              }
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
      return future;
    }
  }

  /**
   * Distributed log consumer.
   */
  private class DefaultDistributedLogConsumer implements DistributedLogConsumer {
    private String leader;
    private long index;
    private volatile Consumer<LogRecord> consumer;

    /**
     * Registers the consumer with the given leader.
     *
     * @param leader the leader with which to register the consumer
     */
    private CompletableFuture<Void> register(String leader) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      this.leader = leader;
      protocol.consume(leader, ConsumeRequest.newBuilder()
          .setMemberId(clientId)
          .setConsumerId(consumerId)
          .setIndex(index + 1)
          .build())
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              if (response.getStatus() == ResponseStatus.OK) {
                future.complete(null);
              } else {
                future.completeExceptionally(new DistributedLogException.Unavailable());
              }
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
      return future;
    }

    /**
     * Handles a records request.
     *
     * @param request the request to handle
     */
    private void handleRecords(RecordsRequest request) {
      if (request.getReset()) {
        index = request.getRecord().getIndex() - 1;
      }
      if (request.getRecord().getIndex() == index + 1) {
        Consumer<LogRecord> consumer = this.consumer;
        if (consumer != null) {
          consumer.accept(request.getRecord());
          index = request.getRecord().getIndex();
        }
      } else {
        protocol.reset(leader, ResetRequest.newBuilder()
            .setMemberId(clientId)
            .setConsumerId(consumerId)
            .setIndex(index + 1)
            .build());
      }
    }

    @Override
    public CompletableFuture<Void> consume(long index, Consumer<LogRecord> consumer) {
      return term().thenCompose(term -> {
        protocol.registerRecordsConsumer(consumerId, this::handleRecords, threadContext);
        this.consumer = consumer;
        this.index = index - 1;
        return register(term.leader());
      });
    }
  }

  /**
   * Default distributed log client builder.
   */
  public static class Builder extends DistributedLogClient.Builder {
    @Override
    public DistributedLogClient build() {
      Logger log = ContextualLoggerFactory.getLogger(DistributedLogClient.class, LoggerContext.builder(DistributedLogClient.class)
          .addValue(clientId)
          .build());

      // If a ThreadContextFactory was not provided, create one and ensure it's closed when the server is stopped.
      boolean closeOnStop;
      ThreadContextFactory threadContextFactory;
      if (this.threadContextFactory == null) {
        threadContextFactory = threadModel.factory("log-client-" + clientId + "-%d", threadPoolSize, log);
        closeOnStop = true;
      } else {
        threadContextFactory = this.threadContextFactory;
        closeOnStop = false;
      }

      return new DefaultDistributedLogClient(
          clientId,
          protocol,
          termProvider,
          threadContextFactory.createContext(),
          threadContextFactory,
          closeOnStop);
    }
  }
}
