#!/bin/sh
SINK_CONNECTOR_NAME="starrocks-kafka-connector"
SINK_CONNECT_URL="http://starrocks:8083/connectors"
SINK_CONFIG_FILE="/connect-configs/starrocks-kafka-connector.json"

# Wait for Sink Kafka Connect to be ready
echo "Waiting for Sink Kafka Connect to be ready..."
until curl -s "$SINK_CONNECT_URL" > /dev/null; do
  echo "Sink Kafka Connect not ready. Retrying in 5 seconds..."
  sleep 5
done

echo "Sink Kafka Connect is ready."

# Check if the connector already exists
if curl -s "$SINK_CONNECT_URL/$SINK_CONNECTOR_NAME" | grep -q "\"name\":\"$SINK_CONNECTOR_NAME\""; then
  echo "Connector '$SINK_CONNECTOR_NAME' already exists. Skipping creation."
else
  echo "Connector '$SINK_CONNECTOR_NAME' does not exist. Creating it..."
  curl -X POST -H "Content-Type: application/json" --data @"$SINK_CONFIG_FILE" "$SINK_CONNECT_URL"
  echo "Connector '$SINK_CONNECTOR_NAME' created."
fi
