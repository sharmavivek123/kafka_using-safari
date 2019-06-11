import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("hello world");

        Logger logger= LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        // create producer properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        //create a producer record


        for(int k=0;k<10;k++) {

            String topic="myfirst_topic";
            String key="key_id_" + Integer.toString(k);
            String value="hi java application  " + Integer.toString(k);

            ProducerRecord<String, String> dataForKafka = new ProducerRecord<>(topic, key,value);

            logger.info("key is :: " + key);
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
            }).get();

        }
        kafkaProducer.flush();
        kafkaProducer.close();


    }
}


// key_0 goes to partition 1
//1 :: 1
//2  : : 1
//3 : : 1
// 4 :: 2
//5 :: 2

// so everytime it choose same partition so by providing key same key goes to same key