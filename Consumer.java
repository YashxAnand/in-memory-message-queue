import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Consumer{
    private final String consumerId;

    public Consumer(){
        this.consumerId = UUID.randomUUID().toString();
    }

    public void consume(Message message) throws MessageConsumptionException{
        try{
            Thread.currentThread().sleep(2, TimeUnit.SECONDS);
        }catch(Exception e){
            throw new MessageConsumptionException(String.format("Error occured while trying to consume message with ID: %s\n", message.getId()));
        }
    }
}