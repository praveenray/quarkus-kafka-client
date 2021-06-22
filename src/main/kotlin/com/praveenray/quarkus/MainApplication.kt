package com.praveenray.quarkus

import com.praveenray.quarkus.services.CSVToKafkaService
import com.praveenray.quarkus.services.KafkaOps
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch

@QuarkusMain
class MainApplication(
    private val kafkaOps: KafkaOps,
    private val csvService: CSVToKafkaService,
    @ConfigProperty(name = "kafka.broker") private val kafkaBroker: String,
    @ConfigProperty(name = "kafka.input.topic") private val inputTopic: String,
    @ConfigProperty(name = "kafka.output.topic") private val outputTopic: String,
) : QuarkusApplication {
    private val logger = Logger.getLogger(MainApplication::class.java)

    override fun run(vararg args: String?): Int {
        println("Starting...")
        kafkaOps.createNeededTopics(kafkaBroker, inputTopic, outputTopic)
        val latch = CountDownLatch(1)
        val topicContent = kafkaOps.readKafkaTopic(kafkaBroker, inputTopic)
        topicContent.onItem().transformToMulti { record ->
            val filePath = Paths.get(String(record.value()))
            csvService.readAccountsFile(filePath)
        }.concatenate().onItem().transformToUniAndConcatenate { row ->
            kafkaOps.publishToTopic(outputTopic, kafkaBroker, row)
        }.subscribe().with({ i ->
            logger.info("Sent ${i.topic}:${i.offset}")
        }, { ex ->
            println("Failed with: $ex")
            latch.countDown()
        }) {
            println("Finished")
            latch.countDown()
        }
        latch.await()
        return 0
    }
}
