package org.example;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.*;


@SuppressWarnings("ALL")
public class Producer {
    static String topic = "itc";
    static KafkaProducer<String, String> producer;
    static Properties p = new Properties();
    static int gvolume = 100000;
    static int inventorytracker = 0;
    static int nvolume;
    static int temperature = 0;
    int api = 0;
    static int level = 0;
    static int capacityfilled = 0;
    static int apisg = 0;
    static int gvolumeusable = 0;
    static int nvolumeusable = 0;

    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        SendData();


    }

    public static void publish(String message) {

        System.out.println("Sending message : " + message);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,
                    message);
            producer.send(record);
            System.out.println(message + record);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    static void SendData() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {

            public void run() {
                String message = "{\"height\":" + 56 + ", \"diameter\":" + 115 + ", \"setpoint\":" + 5.9 + ", \"usablelow\":" + 5.9 + ", \"product\":\"JET-A\",\"tanknominalcapacity\":" + 4200000 + ", \"year\":" + 2003 + ", \"status\":\"Online\", \"gvolume\":" + gvolume + ", \"nvolume\":" + nvolume + ", \"level\":" + level + ", " + "\"temperature\":" + temperature + ", \"capacityfilled\":" + capacityfilled + ", \"inventorytracker\":" + inventorytracker + ", \"apisg\":" + apisg + ", \"gvolumeusable\":" + gvolumeusable + ", \"nvolumeusable\":" + nvolumeusable + "}";
               // String x = message.replaceAll("\"Online\"", "Online");
                publish(message);

                level++;
                temperature = temperature + 2;
                apisg = apisg + 2;
                gvolume = 100000 + gvolume;
                nvolume = 100000 + nvolume;
                gvolumeusable = gvolume - 100000;
                nvolumeusable = nvolume - 100000;
                inventorytracker = 100000 + inventorytracker;
                capacityfilled = 5 + capacityfilled;
                if (capacityfilled >= 100) capacityfilled = 5;
                if (gvolume >= 10000000) gvolume = 100000;
                if (inventorytracker >= 100000000) inventorytracker = 100000;
                if (level >= 28) level = 0;
                if (apisg >= 28) apisg = 0;
                if (nvolume >= 10000000) nvolume = 100000;
                if (temperature >= 180) temperature = 42;
            }
        };
        timer.schedule(task, 2000, 5000);
    }
}

