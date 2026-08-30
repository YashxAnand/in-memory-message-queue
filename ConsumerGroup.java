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

        for(int i = 0; i < MAX_POOL_SIZE; i++)
            availableConsumers.offer(new Consumer());
    }

    public void startConsuming(){
        this.executorService = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        new Thread(()->{
            try{
                while(true){
                    Message message = broker.getMessage(topicName, offset);
                    offset++;

                    Consumer consumer = availableConsumers.take();

                    executorService.submit(()->{
                        try{
                            consumer.consume(message);
                        }catch(MessageConsumptionException e){
                            System.out.println(e.getMessage());
                        }finally{
                            availableConsumers.offer(consumer);
                        }
                    });
                }
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }finally{
                executorService.shutdown();
            }
        }).start();
    }
}