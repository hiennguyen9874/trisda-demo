package com.example.kafka;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationTransformer
    implements Transformer<String, String, KeyValue<String, String>> {
  private final String storeName;
  private KeyValueStore<String, Long> store;
  private ProcessorContext context;

  private static final long EXPIRATION_TIME_MS =
      Duration.ofMinutes(10).toMillis(); // 10-minute TTL
  private static final Duration PUNCTUATE_INTERVAL_MS =
      Duration.ofMinutes(1); // Cleanup every 1 min

  public DeduplicationTransformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = (KeyValueStore<String, Long>)context.getStateStore(storeName);

    if (this.store == null) {
      throw new IllegalStateException("State store is not available!");
    }

    // Schedule periodic cleanup to remove expired object_ids
    context.schedule(
        PUNCTUATE_INTERVAL_MS, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
          long now = System.currentTimeMillis();
          try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
              KeyValue<String, Long> entry = iterator.next();
              if (now - entry.value > EXPIRATION_TIME_MS) {
                store.delete(entry.key); // Remove expired object_id
              }
            }
          }
        });
  }

  @Override
  public KeyValue<String, String> transform(String deviceId, String value) {
    String[] parts = value.split(":");
    String objectId = parts[0]; // Extract object_id

    long currentTimestamp = context.timestamp();
    Long lastSeenTimestamp = store.get(objectId);

    if (lastSeenTimestamp == null || (currentTimestamp - lastSeenTimestamp) >
                                         EXPIRATION_TIME_MS) {
      try {
        store.put(objectId, currentTimestamp);
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Error writing to state store: " + e.getMessage());
      }

      return KeyValue.pair(deviceId, value);
    }

    return null;
  }

  @Override
  public void close() {}
}
