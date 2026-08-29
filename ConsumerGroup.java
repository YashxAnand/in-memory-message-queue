import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerGroup{
    private final LinkedBlockingQueue<Consumer> availableConsumers;
    private IBroker broker;
    private long offset;

    public ConsumerGroup(IBroker broker, int initialConsumers){
        this.availableConsumers = new LinkedBlockingQueue<Consumer>();
        this.broker = broker;
        this.offset = 0;

        for(int i = 0; i < initialConsumers; i++)
            availableConsumers.add(new Consumer());
    }

    public void consume() throws InterruptedException{
        try{
            Message message = broker.getMessage(offset);
            offset++;

            Consumer consumer = availableConsumers.take();

            try{
                consumer.consume(message);
            }catch(MessageConsumptionException e){
                System.out.println(e.getMessage());
            }

            availableConsumers.add(consumer);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    public void addNewConsumer(){
        this.availableConsumers.add(new Consumer());
    }
}