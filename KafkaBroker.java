import java.util.concurrent.ConcurrentHashMap;

public class KafkaBroker implements IBroker{
    private final ConcurrentHashMap<String, Topic> topicMap;

    public KafkaBroker(){
        this.topicMap = new ConcurrentHashMap<>();
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
    }

    @Override
    public Message getMessage(String topicName, long offset) throws InterruptedException{
        Topic topic = topicMap.get(topicName);

        return topic.getMessageAtOffset(offset);
    }
}