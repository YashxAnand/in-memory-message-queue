import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaBroker implements IBroker{
    private final ConcurrentHashMap<String, Topic> topicMap;
    private final ConcurrentHashMap<String, List<ConsumerGroup> > consumerMap;

    public KafkaBroker(){
        this.topicMap = new ConcurrentHashMap<>();
        this.consumerMap = new ConcurrentHashMap<>();
    }
    
    @Override
    public void addMessage(String topicName, String payload){
        Message message = new Message(payload);
        Topic topic = topicMap.get(topicName);

        topic.addMessage(message);
        
        List<ConsumerGroup> consumerGroups = consumerMap.get(topicName);

        for(ConsumerGroup group:consumerGroups){
            group.addMessage(message);
        }
    }

    @Override
    public void addTopic(String topicName){
        topicMap.computeIfAbsent(topicName, (k)->new Topic(topicName));
        consumerMap.computeIfAbsent(topicName, k->new CopyOnWriteArrayList<>());
    }

    @Override
    public void addConsumerGroup(String topicName){
        consumerMap.get(topicName).add(new ConsumerGroup(this, 10));
    }
}