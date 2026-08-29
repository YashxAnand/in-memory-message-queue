
interface IBroker{
    void addMessage(String topic, String payload);

    void addTopic(String topicName);

    void addProducer(String topicName);

    void addConsumerGroup(String topicName);

    Message getMessage(long offset);
}