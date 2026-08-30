import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerGroup{
    private long offset;
    private String topicName;
    private IBroker broker;
    private final LinkedBlockingQueue<Consumer> availableConsumers;
    private final int MAX_POOL_SIZE;
    ExecutorService executorService;

    public ConsumerGroup(IBroker broker, String topicName, int poolSize){
        this.offset = 0l;
        this.broker = broker;
        this.availableConsumers = new LinkedBlockingQueue();
        this.topicName = topicName;
        this.MAX_POOL_SIZE = poolSize;
    }

    public void addToAvailableQueue(Consumer consumer){
        availableConsumers.offer(consumer);
    }

    public void startConsuming(){
        this.executorService = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        while(true){
            Message message = broker.getMessage(topicName, offset);
            offset++;

            Consumer consumer = availableConsumers.take();

            executorService.submit(()->{
                consumer.consume(message);
            });
        }

        this.executorService.shutdown();
    }

    public void addNewConsumer(){
        this.availableConsumers.add(new Consumer(this));
    }
}