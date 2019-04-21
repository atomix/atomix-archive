package io.atomix.core.value.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.core.impl.Metadata;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;

/**
 * Value service.
 */
public class ValueService extends AbstractValueService {
  private AtomicLong version = new AtomicLong();
  private byte[] value = new byte[0];
  private Set<SessionId> listeners = new LinkedHashSet<>();

  @Override
  public SetResponse set(SetRequest request) {
    byte[] previousValue = this.value;
    byte[] nextValue = request.getValue().toByteArray();

    if (Arrays.equals(previousValue, nextValue)) {
      return SetResponse.newBuilder()
          .setMetadata(Metadata.newBuilder()
              .setIndex(getCurrentIndex())
              .build())
          .setVersion(version.get())
          .setPreviousValue(request.getValue())
          .setPreviousVersion(version.get())
          .build();
    }

    this.value = nextValue;
    long previousVersion = version.getAndIncrement();

    ValueEvent event = ValueEvent.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setType(ValueEvent.Type.UPDATED)
        .setPreviousValue(ByteString.copyFrom(previousValue))
        .setPreviousVersion(previousVersion)
        .setNewValue(request.getValue())
        .setNewVersion(version.get())
        .build();
    listeners.forEach(sessionId -> onEvent(sessionId, event));

    return SetResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setVersion(version.get())
        .setPreviousValue(ByteString.copyFrom(previousValue))
        .setPreviousVersion(previousVersion)
        .build();
  }

  @Override
  public GetResponse get(GetRequest request) {
    return GetResponse.newBuilder()
        .setMetadata(Metadata.newBuilder()
            .setIndex(getCurrentIndex())
            .build())
        .setValue(ByteString.copyFrom(value))
        .setVersion(version.get())
        .build();
  }

  @Override
  public CheckAndSetResponse checkAndSet(CheckAndSetRequest request) {
    byte[] previousValue = this.value;
    if (request.getVersion() > 0) {
      if (version.get() == request.getVersion()) {

        byte[] nextValue = request.getUpdate().toByteArray();
        long previousVersion = version.getAndIncrement();
        this.value = nextValue;

        ValueEvent event = ValueEvent.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setType(ValueEvent.Type.UPDATED)
            .setPreviousValue(ByteString.copyFrom(previousValue))
            .setPreviousVersion(previousVersion)
            .setNewValue(request.getUpdate())
            .setNewVersion(version.get())
            .build();
        listeners.forEach(sessionId -> onEvent(sessionId, event));

        return CheckAndSetResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setSucceeded(true)
            .setVersion(version.get())
            .build();
      } else {
        return CheckAndSetResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setSucceeded(false)
            .setVersion(version.get())
            .build();
      }
    } else {
      byte[] checkValue = request.getCheck().toByteArray();
      if (Arrays.equals(previousValue, checkValue)) {
        byte[] nextValue = request.getUpdate().toByteArray();
        long previousVersion = version.getAndIncrement();
        this.value = nextValue;

        ValueEvent event = ValueEvent.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setPreviousValue(ByteString.copyFrom(previousValue))
            .setPreviousVersion(previousVersion)
            .setNewValue(request.getUpdate())
            .setNewVersion(version.get())
            .build();
        listeners.forEach(sessionId -> onEvent(sessionId, event));

        return CheckAndSetResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setSucceeded(true)
            .setVersion(version.get())
            .build();
      } else {
        return CheckAndSetResponse.newBuilder()
            .setMetadata(Metadata.newBuilder()
                .setIndex(getCurrentIndex())
                .build())
            .setSucceeded(false)
            .setVersion(version.get())
            .build();
      }
    }
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
  protected void onExpire(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  protected void onClose(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    AtomicValueSnapshot.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .setVersion(version.get())
        .addAllListeners(listeners.stream().map(SessionId::id).collect(Collectors.toList()))
        .build()
        .writeTo(output);
  }

  @Override
  public void install(InputStream input) throws IOException {
    AtomicValueSnapshot snapshot = AtomicValueSnapshot.parseFrom(input);
    value = snapshot.getValue().toByteArray();
    version.set(snapshot.getVersion());
    listeners = snapshot.getListenersList().stream()
        .map(SessionId::from)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }
}
