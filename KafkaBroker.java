import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    }

    @Override
    public void addTopic(String topicName){
        topicMap.computeIfAbsent(topicName, (k)->new Topic(topicName));
        consumerMap.computeIfAbsent(topicName, k->new CopyOnWriteArrayList<>());
    }

    @Override
    public void addConsumerGroup(String topicName, ConsumerGroup group){
        consumerMap.get(topicName).add(group);
    }

    @Override
    public Message getMessage(String topicName, long offset){
        Topic topic = topicMap.get(topicName);

        return topic.getMessageAtOffset(offset);
    }
}