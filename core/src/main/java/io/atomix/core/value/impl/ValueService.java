package io.atomix.core.value.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.component.Component;
import io.atomix.utils.stream.StreamHandler;

/**
 * Value service.
 */
public class ValueService extends AbstractValueService {
  public static final Type TYPE = new Type();

  /**
   * Value service type.
   */
  @Component
  public static class Type implements ServiceType {
    private static final String NAME = "value";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
      return new ValueService();
    }
  }

  private AtomicLong version = new AtomicLong();
  private byte[] value = new byte[0];

  @Override
  public SetResponse set(SetRequest request) {
    byte[] previousValue = this.value;
    byte[] nextValue = request.getValue().toByteArray();

    if (Arrays.equals(previousValue, nextValue)) {
      return SetResponse.newBuilder()
          .setVersion(version.get())
          .setPreviousValue(request.getValue())
          .setPreviousVersion(version.get())
          .build();
    }

    this.value = nextValue;
    long previousVersion = version.getAndIncrement();

    onEvent(ListenResponse.newBuilder()
        .setType(ListenResponse.Type.UPDATED)
        .setPreviousValue(ByteString.copyFrom(previousValue))
        .setPreviousVersion(previousVersion)
        .setNewValue(request.getValue())
        .setNewVersion(version.get())
        .build());

    return SetResponse.newBuilder()
        .setVersion(version.get())
        .setPreviousValue(ByteString.copyFrom(previousValue))
        .setPreviousVersion(previousVersion)
        .build();
  }

  @Override
  public GetResponse get(GetRequest request) {
    return GetResponse.newBuilder()
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

        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.UPDATED)
            .setPreviousValue(ByteString.copyFrom(previousValue))
            .setPreviousVersion(previousVersion)
            .setNewValue(request.getUpdate())
            .setNewVersion(version.get())
            .build());

        return CheckAndSetResponse.newBuilder()
            .setSucceeded(true)
            .setVersion(version.get())
            .build();
      } else {
        return CheckAndSetResponse.newBuilder()
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

        onEvent(ListenResponse.newBuilder()
            .setType(ListenResponse.Type.UPDATED)
            .setPreviousValue(ByteString.copyFrom(previousValue))
            .setPreviousVersion(previousVersion)
            .setNewValue(request.getUpdate())
            .setNewVersion(version.get())
            .build());

        return CheckAndSetResponse.newBuilder()
            .setSucceeded(true)
            .setVersion(version.get())
            .build();
      } else {
        return CheckAndSetResponse.newBuilder()
            .setSucceeded(false)
            .setVersion(version.get())
            .build();
      }
    }
  }

  @Override
  public void listen(ListenRequest request, StreamHandler<ListenResponse> handler) {
    // Keep the stream open.
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    // Complete the stream.
    StreamHandler<ListenResponse> stream = getCurrentSession().getStream(request.getStreamId());
    if (stream != null) {
      stream.complete();
    }
    return UnlistenResponse.newBuilder().build();
  }

  private void onEvent(ListenResponse event) {
    getSessions()
        .forEach(session -> session.getStreams(ValueOperations.LISTEN_STREAM)
            .forEach(stream -> stream.next(event)));
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    AtomicValueSnapshot.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .setVersion(version.get())
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    AtomicValueSnapshot snapshot = AtomicValueSnapshot.parseFrom(input);
    value = snapshot.getValue().toByteArray();
    version.set(snapshot.getVersion());
  }
}
