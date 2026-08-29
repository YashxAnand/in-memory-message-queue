import java.util.concurrent.ConcurrentHashMap;

public class KafkaBroker implements IBroker{
    private final ConcurrentHashMap<String, Topic> topicMap;
    private final ConcurrentHashMap<String, ConsumerGroup> consumerMap;

    public KafkaBroker(){
        this.topicMap = new ConcurrentHashMap<>();
        this.consumerMap = new ConcurrentHashMap<>();
    }
    
    @Override
    public void addMessage(String topicName, String payload){
        Message message = new Message(payload);
        Topic topic = topicMap.get(topicName);

        topic.addMessage(message);
    }

    @Override
    public void addTopic(String topicName){
        Topic newTopic = topicMap.computeIfAbsent(topicName, (k)->new Topic(topicName));
    }

    @Override
    public void addConsumerGroup(String topicName){
        ConsumerGroup newConsumerGroup = consumerMap.computeIfAbset(topicName, (k)->new ConsumerGroup(this, 10));
    }
}