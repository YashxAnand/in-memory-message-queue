import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Topic{
    private final String topicId;
    private String topicName;
    private ConcurrentHashMap<Long, Message> messages; // Treating index as the offset of the message and assuming we retain all the messages forever.
    private AtomicLong offset;

    public Topic(String topicName){
        this.topicId = UUID.randomUUID().toString();
        this.topicName = topicName;
        this.messages = new ConcurrentHashMap<>();
        this.offset = new AtomicLong(0);
    }

    Message getMessageAtOffset(long offset) throws OffsetNotFoundException{
        try{
            Message message = messages.get(Long.valueOf(offset));

            return message;
        }catch(Exception e){
            throw new OffsetNotFoundException(String.format("Offset %d doesn't exist in the topic %s\n", offset, topicName));
        }
    }

    void addMessage(Message message){
        Long key = Long.valueOf(offset.getAndIncrement());
        messages.put(key, message);
    }
}