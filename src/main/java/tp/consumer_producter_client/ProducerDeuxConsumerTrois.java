package tp.consumer_producter_client;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Console;
import java.time.Duration;
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
            do{
                System.out.print("Entrer votre commande: ");
                cmd = scanner.nextLine();
            }while (!menu(cmd));
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicProducer, cmd);
            producer.send(record, new ProducerCallBack());
            attendReponse = true;
            System.out.println("-----------En attente de réponse-----------");
            while (attendReponse){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(records != null && !records.isEmpty()){
                    attendReponse = false;
                    System.out.println("\n-----------Réponse-----------");
//                    System.out.println(records.iterator().next().value());
//                    try{
//                        //force le commit de l'offset du message sur le bus
//                        consumer.commitSync();
//                    } catch(CommitFailedException e){
//                        e.printStackTrace();
//                    }
                    records.forEach(stringStringConsumerRecord -> {
                        System.out.println(stringStringConsumerRecord.value());
                        try{
                        //force le commit de l'offset du message sur le bus
                            consumer.commitSync();
                        } catch(CommitFailedException e){
                            e.printStackTrace();
                        }
                    });
                    records = null;
                }
            }
            System.out.println("------------------");

        }
        scanner.close();
        producer.close();
        consumer.close();
    }

    private class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }

    private boolean menu(String cmd){
        String[] cmd_split = cmd.split(" ");
        switch (cmd_split[0]){
            case "get_global_values":
                return true;
            case "get_country_values":
                if(cmd_split[1] != null && cmd_split[1].length() == 2)
                    return true;
                else {
                    System.out.println("----------ERROR------------");
                    System.out.println("Le country code est necessaire ou il est incorecte (taille).");
                    System.out.println("--------------------------- ");
                }
            case "get_confirmed_avg":
                return true;
            case "get_deaths_avg":
                return true;
            case "get_countries_deaths_percent":
                return true;
            case "help":
                System.out.println("----------HELP--------------");
                System.out.println("get_global_values : retourne les valeurs globales clés Global du fichier json.");
                System.out.println("get_country_values v_pays : retourne les valeurs du pays demandé ou v_pays est une chaine de caractère du pays demandé (CountryCode, lenght=2).");
                System.out.println("get_confirmed_avg : retourne une moyenne des cas confirmés sum(pays)/nb(pays)");
                System.out.println("get_deaths_avg : retourne une moyenne des Décès sum(pays)/nb(pays)");
                System.out.println("get_countries_deaths_percent : retourne le pourcentage de Décès par rapport aux cas confirmés");
                System.out.println("help : affiche la liste des commandes et une explication.");
                System.out.println("--------------------------- ");
                return false;
            default:
                System.out.println("----------ERROR------------");
                System.out.println("Commande inconnue.\nTaper help pour plus d'information.");
                System.out.println("--------------------------- ");
                return false;

        }
    }

}
