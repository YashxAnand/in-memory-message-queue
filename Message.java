import java.util.UUID;

public class Message{
    private final String messageId;
    private final String payload;

    Message(String payload){
        this.messageId = UUID.randomUUID().toString();
        this.payload = payload;
    }

    public String getId(){
        return messageId;
    }
}