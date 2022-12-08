package telran.java2022;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KafkaConsumerApplication {


    public static void main(String[] args) {

        SpringApplication.run(KafkaConsumerApplication.class, args);

        String bootstrapServers = "glider-01.srvs.cloudkafka.com:9094";
        String groupId = "pem6p4mz-default";
        String topic = "pem6p4mz-default";


        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, "pem6p4mz", "xZySRiiAOeCeKj9ENW6cBOLa-Iq1m6Uf");


        Properties properties;
        String serializer = StringDeserializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", deserializer);
        properties.put("value.deserializer", deserializer);
        properties.put("key.serializer", serializer);
        properties.put("value.serializer", serializer);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("sasl.jaas.config", jaasCfg);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }





//        Properties properties = new Properties();
//
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
////        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//
//        consumer.subscribe(Arrays.asList(topic));
//
//        while(true){
//            ConsumerRecords<String, String> records =
//                    consumer.poll(Duration.ofMillis(100));

//            for (ConsumerRecord<String, String> record : records){
//
//                System.out.println("Key: " + record.key() + ", Value: " + record.value());
//                System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
//            }
//        }
//
//    }



}}
