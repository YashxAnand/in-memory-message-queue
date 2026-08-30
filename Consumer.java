import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Consumer{
    private final String consumerId;
    private final ConsumerGroup consumerGroup;

    public Consumer(ConsumerGroup consumerGroup){
        this.consumerId = UUID.randomUUID().toString();
        this.consumerGroup = consumerGroup;
    }

    public void consume(Message message) throws MessageConsumptionException{
        try{
            Thread.currentThread().sleep(2, TimeUnit.SECONDS);

            consumerGroup.addToAvailableQueue(this);
        }catch(Exception e){
            throw new MessageConsumptionException(String.format("Consumer %s failed while trying to consume message with ID: %s\n", consumerId, message.getId()));
        }
    }
}