import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerGroup{
    private final LinkedBlockingQueue<Consumer> availableConsumers;
    private final LinkedBlockingQueue<Message> messageBuffer;
    private IBroker broker;
    private AtomicLong offset;

    public ConsumerGroup(IBroker broker, int initialConsumers){
        this.availableConsumers = new LinkedBlockingQueue<>();
        this.messageBuffer = new LinkedBlockingQueue<>();
        this.broker = broker;
        this.offset = new AtomicLong(0);

        for(int i = 0; i < initialConsumers; i++)
            availableConsumers.add(new Consumer());
    }

    public void addMessage(Message message){
        messageBuffer.offer(message);
    }

    public void consume() throws InterruptedException{
        // This method will be constantly called by some thread
        try{
            Message message = messageBuffer.take();
            offset.getAndIncrement();

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