package tp;

import tp.consumer_producter_client.ProducerDeuxConsumerTrois;

public class Main{

        public Main() throws InterruptedException {
            ProducerDeuxConsumerTrois c2p3 = new ProducerDeuxConsumerTrois();

            Thread thread = new Thread(c2p3);
            thread.run();

            while (true){

            }
        }

        public static void main(String[] args) throws InterruptedException {
            Main main = new Main();
        }
}