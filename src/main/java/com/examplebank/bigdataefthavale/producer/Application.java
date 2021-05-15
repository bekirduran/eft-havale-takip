package com.examplebank.bigdataefthavale.producer;

import com.examplebank.bigdataefthavale.producer.DataGenerator;
import com.examplebank.bigdataefthavale.producer.MyKafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.FileNotFoundException;



public class Application {



    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
        Producer producer = myKafkaProducer.generate();


        DataGenerator generator = new DataGenerator();

        while (true){
            Thread.sleep(1000);
            String data = generator.generateData();
            ProducerRecord record = new ProducerRecord<String,String>("trace-eft",data);
            producer.send(record);
        }


    }

}
