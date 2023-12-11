package com.ashok.topologies.demo.config

import com.ashok.demos.domain.PurchaseOrder
import io.github.serpro69.kfaker.faker
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec
import org.springframework.integration.kafka.dsl.Kafka
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter.ListenerMode
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.GenericMessage
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

@Configuration
@EnableKafkaStreams
class TopologyIntegrationConfig {

    companion object {
        const val WRITE_TO_KAFKA: String = "to-kafka-producer-template"
    }

    @Autowired
    lateinit var kafkaProperties: KafkaProperties

    @Value("\${app.input-topic}")
    private val inputTopic: String? = null

    @Value("\${app.destination-topic}")
    private val destinationTopic: String? = null


    //Producer
    @Bean
    fun integrationFlow(): IntegrationFlow? {
        return IntegrationFlow
                //Scheduler to trigger the flow for every 1000 ms
            .from({ GenericMessage("") }) { c: SourcePollingChannelAdapterSpec ->
                c.poller(Pollers.fixedDelay(1000))
                    .autoStartup(true)
                    .id("schedulerProducerFlow")
            }

            .transform<String, PurchaseOrder> {
                val faker = faker { }
                //Generating Random Data
                val po = PurchaseOrder( faker.device.modelName(), faker.device.serial(),
                    faker.idNumber.toString(),  generateRandomDate(), faker.random.
                    randomString(10, true))

                println("Streams:: Purchase Order from Avro Profile \t$po")
                po
            }

            //Adding the Payload's name as Kafka Key in the headers.
            .enrichHeaders { headers ->
                headers.headerExpression(KafkaHeaders.KEY, "payload.name") }

            .channel(WRITE_TO_KAFKA)
            .get()
    }

    @Bean
    fun customStreamProcess(kStreamBuilder: StreamsBuilder): KTable<String, Long> {

        val input = kStreamBuilder.stream<String, PurchaseOrder>(inputTopic)

        var table: KTable<String, Long> = input
            .groupByKey()
            .count()
            table.toStream().to(destinationTopic, Produced.with(Serdes.String(), Serdes.Long()))

        // Consume the KTable from the output topic
        val purchaseOrderCounts: KTable<String, Long> = kStreamBuilder.table(
            destinationTopic,
            Consumed.with(Serdes.String(), Serdes.Long())
        )

        // Process the KTable with the Totals by Message Key
        purchaseOrderCounts.toStream()
            .foreach { key, value -> println("Key: $key, Count: $value") }

        return table
    }


    @Bean
    fun kafkaConsumer(): IntegrationFlow {
        return IntegrationFlow
            .from(Kafka.messageDrivenChannelAdapter(concurrentConsumerFactory()
                .createContainer(inputTopic),
                    ListenerMode.record)
                    .id("purchaseOrderConsumerCnf"))

            .transform<GenericRecord, PurchaseOrder> {
                record ->
                    PurchaseOrder(record["name"].toString(),record["code"].toString(),
                            record["id"].toString(),
                                record["purchasedDate"].toString(),
                                        record["customerId"].toString())
            }

            //Perform any operation on the payload
            .handle {
                    po: PurchaseOrder, _: MessageHeaders ->
                    println("Kafka Consumer:: Purchase Order Received for :: $po")
                    po
            }

            .channel("nullChannel")
            .get()

    }

    @Bean
    fun kafkaProducerTemplate(kafkaTemplate: KafkaTemplate<*, *>): IntegrationFlow? {

        if (inputTopic != null) {
            kafkaTemplate.defaultTopic = inputTopic
        }

        return IntegrationFlow.from(WRITE_TO_KAFKA)
            .handle(Kafka.outboundChannelAdapter(kafkaTemplate))
            .get()
    }

    @Bean
    fun concurrentConsumerFactory(): ConcurrentKafkaListenerContainerFactory<String, PurchaseOrder> {

        val props: Map<String, Any> = mutableMapOf<String, Any>().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.consumer.groupId)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.consumer.autoOffsetReset)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.consumer.keyDeserializer)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.consumer.valueDeserializer)
            kafkaProperties.consumer.properties["schema.registry.url"]?.let { put("schema.registry.url", it) }
        }

        return ConcurrentKafkaListenerContainerFactory<String, PurchaseOrder>().apply {
            consumerFactory = DefaultKafkaConsumerFactory(props)
            setConcurrency(3) // Set your concurrency level
        }
    }

    fun generateRandomDate(): String {
        val randomDay = kotlin.random.Random.nextLong(LocalDate.of(2000, 1, 1).toEpochDay(), LocalDate.of(2023, 12, 31).toEpochDay())
        return LocalDate.ofEpochDay(randomDay).format(DateTimeFormatter.ofPattern("\"yyyy-MM-dd"))
    }
}