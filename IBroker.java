
interface IBroker{
    void addMessage(String topic, String payload);

    void addTopic(String topicName);

    void addConsumerGroup(String topicName);
}