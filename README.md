# quarkus kafka project

This project uses Quarkus and Kafka to read from a CSV file and writes it's JSON representation to a kafka topic.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## Running the application in dev mode

- You must have a [kafka instance](https://github.com/praveenray/kafka-docker-cluster) running. Kafka Host must be given as a property in application.properties (kafka.broker)
- The input topic and output topic names are in application.properties (kafka.input.topic, kafka.output.topic)
- Run the program: 
```shell script
./gradlew quarkusDev
```
- Create a CSV file in a directory with headers in the first line (see data/sample.csv)
- Put the full path of this file onto input topic:
  ```
    kafkacat -b localhost:9092 -P
    <enter full path to data/sample.csv followed by CTRL+D>
   ```
- If all goes well, there should be JSON on the output.topic:
```kafkacat -b <your kafka host:port> -o beginning -e -t output.topic```
