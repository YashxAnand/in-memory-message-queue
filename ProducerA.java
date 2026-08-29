
public class ProducerA implements IProducer{
    IBroker broker;

    ProducerA(IBroker broker){
        this.broker = broker;
    }

    @Override
    public void sendMessage(String topic, String message){
        broker.addMessage(topic, message);
    }
}