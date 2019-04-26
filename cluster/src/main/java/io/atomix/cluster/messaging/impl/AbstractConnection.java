package io.atomix.cluster.messaging.impl;

import io.atomix.cluster.messaging.MessagingException;
import io.atomix.utils.stream.StreamHandler;

/**
 * Abstract connection.
 */
public abstract class AbstractConnection implements Connection {
  protected final HandlerRegistry<Long, StreamHandler<byte[]>> streamHandlers = new HandlerRegistry<>();

  @Override
  public void registerStreamHandler(long id, StreamHandler<byte[]> handler) {
    streamHandlers.register(id, handler);
  }

  @Override
  public StreamHandler<byte[]> unregisterStreamHandler(long id) {
    return streamHandlers.unregister(id);
  }

  protected void dispatch(ProtocolStream message) {
    StreamHandler<byte[]> handler = streamHandlers.get(message.id());
    if (handler != null) {
      handler.next(message.payload());
    }
  }

  protected void dispatch(ProtocolStreamEnd message) {
    StreamHandler<byte[]> handler = streamHandlers.unregister(message.id());
    if (handler != null) {
      if (message.status() == ProtocolStatus.OK) {
        handler.complete();
      } else if (message.status() == ProtocolStatus.ERROR_HANDLER_EXCEPTION) {
        handler.error(new MessagingException.RemoteHandlerFailure());
      } else if (message.status() == ProtocolStatus.ERROR_NO_HANDLER) {
        handler.error(new MessagingException.NoRemoteHandler());
      }
    }
  }
}
