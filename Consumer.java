import java.util.UUID;

public class Consumer{
    private final String consumerId;

    public Consumer(){
        this.consumerId = UUID.randomUUID().toString();
    }

    public void consume(Message message) throws MessageConsumptionException{
        try{
            Thread.sleep(2000);
        }catch(Exception e){
            throw new MessageConsumptionException(String.format("Consumer %s failed while trying to consume message with ID: %s\n", consumerId, message.getId()));
        }
    }
}