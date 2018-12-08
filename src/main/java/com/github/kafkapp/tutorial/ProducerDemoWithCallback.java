package com.github.kafkapp.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());
    public static void main(String[] args) {
        //Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka Producer
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i<10; i++) {
            //Kafka Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "From Java "+i);

            //Send Message
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        log.error("Message not saved on Kafka with error", e);
                        return;
                    }

                    log.info("Topic {} has got the message in Partition {}.", recordMetadata.topic(), recordMetadata.partition());
                }
            });
        }
        producer.close();
    }
}
