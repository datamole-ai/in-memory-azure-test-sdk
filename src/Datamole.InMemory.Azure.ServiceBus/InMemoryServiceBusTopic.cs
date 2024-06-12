using System.Collections.Concurrent;

using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus;

public class InMemoryServiceBusTopic(string topicName, InMemoryServiceBusEntityOptions options, InMemoryServiceBusNamespace serviceBusNamespace)
    : InMemoryServiceBusEntity(topicName, options, serviceBusNamespace)
{
    private readonly ConcurrentDictionary<string, InMemoryServiceBusTopicSubscription> _subscriptions = new();

    public string TopicName { get; } = topicName;

    internal override void AddMessage(ServiceBusMessage message)
    {
        foreach (var subscription in _subscriptions.Values)
        {
            subscription.MessageStore.AddMessage(message);
        }
    }

    internal override void AddMessages(IReadOnlyList<ServiceBusMessage> messages)
    {
        foreach (var (_, subscription) in _subscriptions)
        {
            subscription.MessageStore.AddMessages(messages);
        }
    }

    public InMemoryServiceBusTopicSubscription? FindSubscription(string subscriptionName)
    {
        if (!_subscriptions.TryGetValue(subscriptionName, out var subscription))
        {
            return null;
        }

        return subscription;
    }

    public InMemoryServiceBusTopicSubscription AddSubscription(string subscriptionName)
    {
        var subscription = new InMemoryServiceBusTopicSubscription(subscriptionName, this);

        if (!_subscriptions.TryAdd(subscriptionName, subscription))
        {
            throw new InvalidOperationException($"Subscription '{subscriptionName}' already exists.");
        }

        return subscription;
    }


}
