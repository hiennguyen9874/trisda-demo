package com.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode; // <-- Ensure this import is included
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class KafkaStreamsApp {
  private static final String INPUT_TOPIC = "tris-road";
  private static final String OUTPUT_TOPIC = "tris_road_count";
  private static final String STATE_STORE_NAME = "tris-road";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.1.41:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
              Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
              Serdes.String().getClass());
    // props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
    //           EventTimeExtractor.class.getName());

    StreamsBuilder builder = new StreamsBuilder();

    Serde<JsonNode> jsonSerde = Serdes.serdeFrom(
        new JsonSerializer<>(), new JsonDeserializer<>(JsonNode.class));

    // Add a state store for tracking seen object_ids
    builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(),
        Serdes.Long()));

    KStream<String, String> stream = builder.stream(
        INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, String> deduplicatedStream =
        stream
            .flatMap((key, value) -> {
              try {
                JsonNode jsonNode = MAPPER.readTree(value);
                String deviceId = jsonNode.get("deviceid").asText();
                List<KeyValue<String, String>> results = new ArrayList<>();

                for (JsonNode object : jsonNode.get("objects")) {
                  String objectId = object.get("object_id").asText();
                  String label = object.get("class-label").asText();
                  String gieId = object.get("gie_id").asText();

                  if (gieId != null && gieId != "1")
                    continue;

                  results.add(KeyValue.pair(deviceId, objectId + ":" + label));
                }
                return results;
              } catch (Exception e) {
                // e.printStackTrace();
                System.err.println("Error processing message: " +
                                   e.getMessage());
                return List.of();
              }
            })
            .transform(()
                           -> new DeduplicationTransformer(STATE_STORE_NAME),
                       STATE_STORE_NAME)
            .mapValues(value -> value.split(":")[1]);

    deduplicatedStream.groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .aggregate(
            ()
                -> MAPPER.createObjectNode(), // Initial empty JSON object
            (deviceId, label, aggregate)
                -> {
              int currentCount =
                  aggregate.has(label) ? aggregate.get(label).asInt() : 0;
              ((ObjectNode)aggregate).put(label, currentCount + 1);
              return aggregate;
            },
            Materialized.with(Serdes.String(), jsonSerde))
        .toStream()
        .map((windowedKey, counts) -> {
          ObjectNode result = MAPPER.createObjectNode();
          result.put("deviceid", windowedKey.key());
          // result.put("window_start", windowedKey.window().start());
          // result.put("window_end", windowedKey.window().end());
          result.put("window_start", FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().start())));
          result.put("window_end", FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().end())));
          // Flatten the label_counts
          counts.fields().forEachRemaining(
              entry -> result.put(entry.getKey(), entry.getValue().asInt()));
          return KeyValue.pair(windowedKey.key(), result.toString());
        })
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    System.out.println("Kafka Streams application started...");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down Kafka Streams...");
      streams.close();
    }));
  }

  public static class EventTimeExtractor implements TimestampExtractor {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS")
            .withZone(ZoneId.of("UTC"));

    @Override
    public long extract(ConsumerRecord<Object, Object> record,
                        long partitionTime) {
      try {
        JsonNode jsonNode = MAPPER.readTree(record.value().toString());
        String datetime = jsonNode.get("datetime").asText();
        Instant instant = Instant.from(FORMATTER.parse(datetime));
        return instant.toEpochMilli();
      } catch (Exception e) {
        System.err.println("Error extracting timestamp: " + e.getMessage());
        return partitionTime;
      }
    }
  }
}
