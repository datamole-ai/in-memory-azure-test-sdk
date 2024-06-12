using System.Runtime.CompilerServices;

using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus.Clients.Internals;

internal static class ServiceBusClientUtils
{
    public static async IAsyncEnumerable<ServiceBusReceivedMessage> ReceiveAsAsyncEnumerable(PlainMessageStore store, ServiceBusReceiveMode receiveMode, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await store.ReceiveAsync(16, TimeSpan.FromSeconds(8), receiveMode, cancellationToken);

            foreach (var message in messages)
            {
                yield return message;
            }
        }
    }

    public static async IAsyncEnumerable<ServiceBusReceivedMessage> ReceiveAsAsyncEnumerable(LockedSession session, ServiceBusReceiveMode receiveMode, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await session.Store.ReceiveAsync(session, 16, TimeSpan.FromSeconds(8), receiveMode, cancellationToken);

            if (!result.IsSuccessful)
            {
                throw ServiceBusClientExceptionFactory.SessionReceiveFailed(result.Error.Value, session.FullyQualifiedNamesace, session.EntityPath, session.SessionId);
            }

            foreach (var message in result.Value)
            {
                yield return message;
            }
        }
    }


    public static async Task<ServiceBusReceivedMessage?> ReceiveSingleAsync(PlainMessageStore store, TimeSpan maxWaitTime, ServiceBusReceiveMode receiveMode, CancellationToken cancellationToken)
    {
        var messages = await store.ReceiveAsync(1, maxWaitTime, receiveMode, cancellationToken);
        return messages.SingleOrDefault();
    }

    public static async Task<ServiceBusReceivedMessage?> ReceiveSingleAsync(LockedSession session, TimeSpan maxWaitTime, ServiceBusReceiveMode receiveMode, CancellationToken cancellationToken)
    {
        var result = await session.Store.ReceiveAsync(session, 1, maxWaitTime, receiveMode, cancellationToken);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionReceiveFailed(result.Error.Value, session.FullyQualifiedNamesace, session.EntityPath, session.SessionId);
        }

        return result.Value.SingleOrDefault();
    }

    public static InMemoryServiceBusTopic GetTopic(string fullyQualifiedNamespace, string topicName, InMemoryServiceBusProvider provider)
    {
        var ns = GetNamespace(fullyQualifiedNamespace, provider);
        var topic = ns.FindTopic(topicName);

        if (topic is null)
        {
            throw ServiceBusClientExceptionFactory.MessagingEntityNotFound(fullyQualifiedNamespace, topicName);
        }

        return topic;
    }

    public static InMemoryServiceBusQueue GetQueue(string fullyQualifiedNamespace, string queueName, InMemoryServiceBusProvider provider)
    {
        var ns = GetNamespace(fullyQualifiedNamespace, provider);
        var queue = ns.FindQueue(queueName);

        if (queue is null)
        {
            throw ServiceBusClientExceptionFactory.MessagingEntityNotFound(fullyQualifiedNamespace, queueName);
        }

        return queue;
    }

    public static InMemoryServiceBusEntity GetEntity(string fullyQualifiedNamespace, string entityName, InMemoryServiceBusProvider provider)
    {
        var ns = GetNamespace(fullyQualifiedNamespace, provider);
        var entity = ns.FindEntity(entityName);

        if (entity is null)
        {
            throw ServiceBusClientExceptionFactory.MessagingEntityNotFound(fullyQualifiedNamespace, entityName);
        }

        return entity;
    }

    public static InMemoryServiceBusNamespace GetNamespace(string fullyQualifiedNamespace, InMemoryServiceBusProvider provider)
    {
        if (!provider.TryGetNamespace(fullyQualifiedNamespace, out var serviceBusNamespace))
        {
            throw ServiceBusClientExceptionFactory.NamespaceNotFound(fullyQualifiedNamespace);
        }

        return serviceBusNamespace;
    }

    public static InMemoryServiceBusTopicSubscription GetSubscription(string fullyQualifiedNamespace, string topicName, string subscriptionName, InMemoryServiceBusProvider provider)
    {
        var topic = GetTopic(fullyQualifiedNamespace, topicName, provider);
        var subscription = topic.FindSubscription(subscriptionName);

        if (subscription is null)
        {
            throw ServiceBusClientExceptionFactory.MessagingEntityNotFound(fullyQualifiedNamespace, topicName, subscriptionName);
        }

        return subscription;
    }

    public static PlainMessageStore GetStoreForQueue(string fullyQualifiedNamespace, string queueName, InMemoryServiceBusProvider provider)
    {
        var queue = GetQueue(fullyQualifiedNamespace, queueName, provider);

        var store = queue.MessageStore as PlainMessageStore;

        return store ?? throw ServiceBusClientExceptionFactory.SessionsNotEnabled(fullyQualifiedNamespace, queueName);
    }

    public static PlainMessageStore GetStoreForTopic(string fullyQualifiedNamespace, string topicName, string subscriptionName, InMemoryServiceBusProvider provider)
    {
        var subscription = GetSubscription(fullyQualifiedNamespace, topicName, subscriptionName, provider);

        var store = subscription.MessageStore as PlainMessageStore;

        return store ?? throw ServiceBusClientExceptionFactory.SessionsNotEnabled(fullyQualifiedNamespace, topicName, subscriptionName);
    }

}
