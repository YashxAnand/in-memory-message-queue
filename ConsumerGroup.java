import java.util.ArrayDeque;

public class ConsumerGroup{
    Queue<Consumer> availableConsumers;
    Queue<Consumer> busyConsumers;

    public ConsumerGroup(int initialConsumers){
        this.availableConsumers = new ArrayDeque<Consumer>();
        this.busyConsumers = new ArrayDeque<Consumer>();

        for(int i = 0; i < initialConsumers; i++)
            availableConsumers.add(new Consumer());
    }

    public void addNewConsumer(){
        this.availableConsumers.add(new Consumer());
    }
}