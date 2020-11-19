package tp.consumer_producter_client;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerFactory {

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);

    }




}
