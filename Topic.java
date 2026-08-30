import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Topic{
    private final String topicId;
    private String topicName;
    private ConcurrentHashMap<Long, Message> messages; // Treating index as the offset of the message and assuming we retain all the messages forever.
    private AtomicLong offset;
    private final ReentrantLock lock;
    private final Condition condition;

    public Topic(String topicName){
        this.topicId = UUID.randomUUID().toString();
        this.topicName = topicName;
        this.messages = new ConcurrentHashMap<>();
        this.offset = new AtomicLong(0);
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    Message getMessageAtOffset(long offset) throws InterruptedException{
        lock.lock();

        try{
            while(offset > this.offset.get()){
                condition.await();
            }

            return messages.get(Long.valueOf(offset));
        }finally{
            lock.unlock();
        }

    }

    void addMessage(Message message){
        lock.lock();

        try{
            Long key = Long.valueOf(offset.getAndIncrement());
            messages.put(key, message);
            condition.signalAll();
        }finally{
            lock.unlock();
        }
    }
}