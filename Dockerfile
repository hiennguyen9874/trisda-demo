# Use OpenJDK as the base image
FROM openjdk:11-jre-slim

# Set the working directory
WORKDIR /app

# Copy the JAR file (build this using Maven/Gradle first)
COPY ./target/kafka-streams-app-1.0-SNAPSHOT.jar /app/kafka-streams-app.jar

# Expose the port (optional, if your app exposes a REST endpoint for metrics or monitoring)
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["java", "-jar", "/app/kafka-streams-app.jar"]
