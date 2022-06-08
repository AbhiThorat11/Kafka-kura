package org.example;

import com.fasterxml.jackson.databind.JsonNode;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String token = "iAKdFnHbSKogT-zusbbFNheTnlc9WsT1ypx16mcgdYsmwxrrFu7eRc622syL91r7_JGddqku-3nEDA-SWGicQQ==";
        String bucket = "HelloDB";
        String org = "ITC Infotech";
        String topic = "itc";

        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-json");

        KafkaConsumer<String, JsonNode> kafkaConsumer = new KafkaConsumer<String, JsonNode>(props);
        kafkaConsumer.subscribe(Arrays.asList(topic));


        while (true) {
            ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(Duration.ofMillis(300));
            if(records.count()>0){
                for (ConsumerRecord<String, JsonNode> record : records) {

                    System.out.println("consumer records " + records);

                   System.out.println("rec in consumer " + record.value());
                    String jsonNode1 = String.valueOf(record.value().get("status"));
                 //   JsonNode jsonNode2 = record.value().get("tank1_Temperature(F)");
                 //   JsonNode j1 = jsonNode2.get("tank1_temp");
                 //   JsonNode jsonNode3 = record.value().get("height");
                 //   JsonNode jsonNode4 = record.value().get("diameter");


                    Point point =Point.measurement("dem01")
                            .addField("Status", Integer.valueOf(jsonNode1));
                 //   point.addField("Temp", (String.valueOf(jsonNode2)));

                    WriteApiBlocking writeApi = client.getWriteApiBlocking();
                    writeApi.writePoint(bucket, org, point);





                }
            } else {
                System.out.println("no records in consumer");
            }
        }
    }
}