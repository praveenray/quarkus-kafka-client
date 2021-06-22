package com.praveenray.quarkus.services

import com.fasterxml.jackson.databind.ObjectMapper
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.infrastructure.Infrastructure
import io.smallrye.mutiny.subscription.MultiEmitter
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
class KafkaOps(
    private val jackson: ObjectMapper,
    @ConfigProperty(name="kafka.group.id") private val kafkaGroupId: String,
) {
    private val logger = Logger.getLogger(KafkaOps::class.java)

    fun createNeededTopics(kafkaBroker: String, inputTopic: String, outputTopic: String): Uni<Void> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBroker
        val adminClient = AdminClient.create(props)
        val topics = setOf(
            inputTopic,
            outputTopic,
        )
        var toCreate: List<NewTopic>
        logger.debug("Proceeding with topic creation")
        return Uni.createFrom().future(adminClient.listTopics(ListTopicsOptions().listInternal(false)).listings())
            .onItem().transformToUni { topicListing ->
                toCreate = topics.filter { topic -> topicListing.find { it.name() == topic } == null }.map {
                    NewTopic(
                        it,
                        1,
                        1
                    )
                }
                if (toCreate.isNotEmpty()) {
                    logger.warn("Creating following new topics: ${toCreate.map {it.name()}.joinToString(",")}")
                    Uni.createFrom().future(adminClient.createTopics(toCreate).all())
                } else {
                    Uni.createFrom().voidItem()
                }
            }
    }

    fun publishToTopic(topic: String, kafkaBroker: String, data: Map<String,Any>): Uni<RecordPublishStatus> {
        val props = createProducerProps(kafkaBroker, false)
        val producer = KafkaProducer<String,String>(props)
        val record = ProducerRecord(topic, UUID.randomUUID().toString(), jackson.writeValueAsString(data))
        return Uni.createFrom().emitter { em ->
            producer.send(record) { rs, e: Exception? ->
                if (e != null) {
                    em.fail(e)
                } else {
                    em.complete(
                        RecordPublishStatus(
                            offset = rs.offset(),
                            topic = topic,
                            partition = rs.partition(),
                        )
                    )
                }
            }
        }
    }

    fun readKafkaTopic(kafkaBroker: String, topic: String): Multi<ConsumerRecord<String, ByteArray>> {
        val props = createConsumerProps(kafkaBroker, kafkaGroupId, false)
        val consumer = KafkaConsumer<String, ByteArray>(props)
        consumer.subscribe(listOf(topic))

        return Multi.createFrom().emitter<ConsumerRecord<String, ByteArray>?> { em ->
            val state = AtomicReference("continue")
            while(true) {
                when (state.get()) {
                    "continue" -> {
                        logger.info("Polling Kafka Topic: $topic")
                        val toEmit = consumer.poll(Duration.ofMinutes(3)).map { record ->
                            if (record.key() == "STOP") {
                                logger.info("Stop message received.")
                                null
                            } else {
                                record
                            }
                        }
                        if (toEmit.contains(null)) {
                            state.set("stop-received")
                            logger.info("Sending sync commit")
                            commit(consumer, em)
                            em.complete()
                        } else {
                            toEmit.forEach { em.emit(it) }
                            commit(consumer, em)
                        }
                    }
                    "stop-received" -> {
                        break
                    }
                }
            }
        }.runSubscriptionOn(Infrastructure.getDefaultExecutor())
    }

    private fun commit(consumer: KafkaConsumer<String, ByteArray>, emitter: MultiEmitter<in ConsumerRecord<String, ByteArray>>) {
        try {
            consumer.commitSync()
        } catch (e: Exception) {
            emitter.fail(e)
        }
    }

    private fun createProducerProps(
        broker: String,
        isAvro: Boolean,
        schemaRegistryUrl: String? = null
    ): Properties {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return props
    }

    private fun createConsumerProps(
        broker: String,
        consumerGroupId: String,
        isAvro: Boolean,
        schemaRegistryUrl: String? = null
    ): Properties {
        val props = Properties()
        props["group.id"] = consumerGroupId
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = if (isAvro) {
            "io.confluent.kafka.serializers.KafkaAvroDeserializer"
        } else {
            ByteArrayDeserializer::class.java
        }
        if (!schemaRegistryUrl.isNullOrBlank()) {
            props["schema.registry.url"] = schemaRegistryUrl
        }
        props["enable.auto.commit"] = "false"
        props["auto.offset.reset"] = "earliest"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = ByteArrayDeserializer::class.java
        return props
    }
}

data class RecordPublishStatus(
    val offset: Long,
    val partition: Int,
    val topic: String,
    val timestamp: LocalDateTime = LocalDateTime.now()
)