
public class Topic{
    private final String topicId;
    private String topicName;
    private ArrayList<Message> messages; // Treating index as the offset of the message and assuming we retain all the messages forever.

    public Topic(String topicName){
        this.topicId = UUID.randomUUID().toString();
        this.topicName = topicName;
        this.messages = new ArrayList<>();
    }

    Message getMessageAtOffset(long offset) throws OffsetNotFoundException{
        try{
            Message message = messages.get((int)offset);

            return message;
        }catch(Exception e){
            throw new OffsetNotFoundException(String.format("Offset %d doesn't exist in the topic %s\n", offset, topicName));
        }
    }

    void addMessage(Message message){
        messages.add(message);
    }
}