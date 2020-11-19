package tp.consumer_producter_client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Console;
import java.util.Collections;
import java.util.Scanner;

public class ProducerDeuxConsumerTrois implements Runnable {

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private String topicProducer = "Topic2";
    private String topicConsumer = "Topic3";
    private Scanner scanner;

    public ProducerDeuxConsumerTrois() {
        producer = ProducerFactory.createProducer();
        consumer = ConsumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(topicConsumer));
        scanner = new Scanner(System.in);
    }

    @Override
    public synchronized void run() {
        String cmd;

        boolean attendReponse = false;
        while (!Thread.interrupted()){
//            Console console = System.console();
//            cmd = console.readLine("Entrer votre commande: ");

            System.out.print("Entrer votre commande: ");
            cmd = scanner.nextLine();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicProducer, cmd);
            producer.send(record, new ProducerCallBack());
            attendReponse = true;
            System.out.println("-----------Réponse-----------");
//            while (attendReponse){
//                ConsumerRecords<String, String> records = consumer.poll(100);
//                if(records != null && !records.isEmpty()){
//                    attendReponse = false;
//                    records.forEach(message -> {
//                        System.out.println(message.toString());
//                    });
//                }
//            }

        }
        scanner.close();
    }

    private class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }

}
