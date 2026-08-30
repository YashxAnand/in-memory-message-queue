
interface IBroker{
    void addMessage(String topic, String payload);

    void addTopic(String topicName);

    Message getMessage(String topicName, long offset) throws InterruptedException;
}