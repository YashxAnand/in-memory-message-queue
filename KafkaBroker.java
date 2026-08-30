import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaBroker implements IBroker{
    private final ConcurrentHashMap<String, Topic> topicMap;
    private final ConcurrentHashMap<String, List<ConsumerGroup> > consumerMap;
    private final ConcurrentHashMap<String, Lock> topicLocks;
    private final ConcurrentHashMap<String, Condition> topicConditions;

    public KafkaBroker(){
        this.topicMap = new ConcurrentHashMap<>();
        this.consumerMap = new ConcurrentHashMap<>();
        this.topicLocks = new ConcurrentHashMap<>();
        this.topicConditions = new ConcurrentHashMap<>();
    }
    
    @Override
    public void addMessage(String topicName, String payload){
        Message message = new Message(payload);
        Topic topic = topicMap.get(topicName);

        topic.addMessage(message);
        
        Condition topicCondition = topicConditions.get(topicName);
        topicCondition.signalAll();
    }

    @Override
    public void addTopic(String topicName){
        topicMap.computeIfAbsent(topicName, (k)->new Topic(topicName));
        consumerMap.computeIfAbsent(topicName, k->new CopyOnWriteArrayList<>());
        ReentrantLock lock = topicLocks.computeIfAbsent(topicName, k-> new ReentrantLock());
        topicConditions.computeIfAbsent(topicName, k->lock.newCondition());
    }

    @Override
    public void addConsumerGroup(String topicName, ConsumerGroup group){
        consumerMap.get(topicName).add(group);
    }

    @Override
    public Message getMessage(String topicName, long offset){
        Topic topic = topicMap.get(topicName);
        Condition topicCondition = topicConditions.get(topicName);

        while(true){
            try{
                return topic.getMessageAtOffset(offset);
            }catch(OffsetNotFoundException e){
                topicCondition.await();
            }
        }
    }
}