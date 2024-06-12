using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus;

public class InMemoryServiceBusTopicSubscription(string subscriptionName, InMemoryServiceBusTopic parentTopic) : IEntityIdentity
{
    public string SubscriptionName { get; } = subscriptionName;
    public InMemoryServiceBusTopic ParentTopic { get; } = parentTopic;

    public string FullyQualifiedNamespace => ParentTopic.Namespace.FullyQualifiedNamespace;

    public string EntityPath => ParentTopic.TopicName;

    public long ActiveMessageCount => MessageStore.ActiveMessageCount;
    public long MessageCount => MessageStore.MessageCount;

    internal IMessageStore MessageStore { get; } = MessageStoreFactory.CreateMessageStore(parentTopic);
}


