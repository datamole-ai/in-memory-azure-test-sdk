using System.Collections.Concurrent;

namespace Datamole.InMemory.Azure.ServiceBus;

public class InMemoryServiceBusNamespace(string fullyQualifiedNamespace, InMemoryServiceBusProvider provider)
{
    private readonly ConcurrentDictionary<string, InMemoryServiceBusEntity> _entities = new();

    public string FullyQualifiedNamespace { get; } = fullyQualifiedNamespace;
    public InMemoryServiceBusProvider Provider { get; } = provider;

    public TimeProvider TimeProvider => Provider.TimeProvider;

    public InMemoryServiceBusEntity? FindEntity(string entityName) => _entities.TryGetValue(entityName, out var entity) ? entity : null;

    public InMemoryServiceBusQueue? FindQueue(string queueName) => FindEntity(queueName) as InMemoryServiceBusQueue;

    public InMemoryServiceBusTopic? FindTopic(string topicName) => FindEntity(topicName) as InMemoryServiceBusTopic;

    public InMemoryServiceBusQueue AddQueue(string queueName, InMemoryServiceBusEntityOptions? options = null)
    {
        var queue = new InMemoryServiceBusQueue(queueName, options ?? new(), this);

        if (!_entities.TryAdd(queueName, queue))
        {
            throw new InvalidOperationException($"The namespace '{FullyQualifiedNamespace}' already contains entity '{queueName}'.");
        }

        return queue;
    }

    public InMemoryServiceBusTopic AddTopic(string topicName, InMemoryServiceBusEntityOptions? options = null)
    {
        var topic = new InMemoryServiceBusTopic(topicName, options ?? new(), this);

        if (!_entities.TryAdd(topicName, topic))
        {
            throw new InvalidOperationException($"The namespace '{FullyQualifiedNamespace}' already contains entity '{topicName}'.");
        }

        return topic;
    }
}
