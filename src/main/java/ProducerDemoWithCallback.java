import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("hello world");

        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // create producer properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        //create a producer record


        for(int k=0;k<10;k++) {


            ProducerRecord<String, String> dataForKafka = new ProducerRecord<>("myfirst_topic", "hi java application  " + Integer.toString(k));

            //send data


            kafkaProducer.send(dataForKafka, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadat :: \n" +
                                "Topic :: " + recordMetadata.topic() + "\n" +
                                "partition :: " + recordMetadata.partition() + "\n" +
                                "Offset:: " + recordMetadata.offset() + "\n" +
                                "timestamp :: " + recordMetadata.timestamp()
                        );
                    } else
                        logger.info("problem during producing data");
                }
            });

        }
        kafkaProducer.flush();
        kafkaProducer.close();


    }
}
