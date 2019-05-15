package io.atomix.primitive.partition;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import io.atomix.primitive.event.AsyncListenable;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;

/**
 * Partition metadata service.
 */
public interface PartitionMetadataService extends AsyncListenable<PartitionMetadataEvent> {

  /**
   * Gets the partition metadata for the given group.
   *
   * @param partitionGroup the partition group
   * @param decoder        the metadata decoder
   * @param <T>            the metadata type
   * @return a future to be completed with the partition metadata for the given group
   */
  <T> CompletableFuture<T> get(String partitionGroup, ByteArrayDecoder<T> decoder);

  /**
   * Updates the partition group metadata.
   *
   * @param partitionGroup the partition group
   * @param decoder        the metadata decoder
   * @param function       the update function
   * @param encoder        the metadata encoder
   * @param <T>            the metadata type
   * @return a future to be completed once the update is successful
   */
  <T> CompletableFuture<T> update(
      String partitionGroup,
      ByteArrayDecoder<T> decoder,
      Function<T, T> function,
      ByteArrayEncoder<T> encoder);

  /**
   * Deletes the partition metadata.
   *
   * @param partitionGroup the partition group
   * @return a future to be completed once the metadata has been deleted
   */
  CompletableFuture<Boolean> delete(String partitionGroup);

  /**
   * Adds a listener for the given partition group.
   *
   * @param partitionGroup the partition group for which to add the listener
   * @param decoder        the metadata decoder
   * @param listener       the listener to add
   * @return a future to be completed once the listener has been added
   */
  <T> CompletableFuture<Void> addListener(
      String partitionGroup, ByteArrayDecoder<T> decoder, Consumer<PartitionMetadataEvent<T>> listener);

  /**
   * Removes a listener for the given partition group.
   *
   * @param partitionGroup the partition group for which to remove the listener
   * @param listener       the listener to remove
   * @return a future to be completed once the listener has been removed
   */
  <T> CompletableFuture<Void> removeListener(String partitionGroup, Consumer<PartitionMetadataEvent<T>> listener);

}
