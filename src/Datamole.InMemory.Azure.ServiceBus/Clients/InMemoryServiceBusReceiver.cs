using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Clients.Internals;
using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus.Clients;

public class InMemoryServiceBusReceiver : ServiceBusReceiver
{
    private readonly string _identifier;
    private readonly TimeSpan _defaultMaxWaitTime;
    private readonly int _prefetchCount;

    private bool _isClosed = false;

    private Func<PlainMessageStore> GetStore { get; }

    public override string FullyQualifiedNamespace { get; }
    public override string EntityPath { get; }
    public override ServiceBusReceiveMode ReceiveMode { get; }
    public override int PrefetchCount => _prefetchCount;
    public override string Identifier => _identifier;
    public override bool IsClosed => _isClosed;


    private InMemoryServiceBusReceiver(
        string fullyQualifiedNamespace,
        string entityPath,
        ServiceBusReceiverOptions options,
        TimeSpan defaultMaxWaitTime,
        Func<PlainMessageStore> getStore)
    {
        FullyQualifiedNamespace = fullyQualifiedNamespace;
        EntityPath = entityPath;
        ReceiveMode = options.ReceiveMode;
        _prefetchCount = options.PrefetchCount;
        _defaultMaxWaitTime = defaultMaxWaitTime;
        _identifier = options.Identifier ?? Guid.NewGuid().ToString();
        GetStore = getStore;
    }

    internal InMemoryServiceBusReceiver(
        string fullyQualifiedNamespace,
        string queueName,
        ServiceBusReceiverOptions options,
        TimeSpan defaultMaxWaitTime,
        InMemoryServiceBusProvider provider)
        : this(
              fullyQualifiedNamespace,
              queueName,
              options,
              defaultMaxWaitTime, () => ServiceBusClientUtils.GetStoreForQueue(fullyQualifiedNamespace, queueName, provider))
    { }

    internal InMemoryServiceBusReceiver(
        string fullyQualifiedNamespace,
        string topicName,
        string subscriptionName,
        ServiceBusReceiverOptions options,
        TimeSpan defaultMaxWaitTime,
        InMemoryServiceBusProvider provider)
        : this(
              fullyQualifiedNamespace,
              topicName,
              options,
              defaultMaxWaitTime,
              () => ServiceBusClientUtils.GetStoreForTopic(fullyQualifiedNamespace, topicName, subscriptionName, provider))
    { }

    public override async Task CloseAsync(CancellationToken cancellationToken = default) => await DisposeAsync();

    public override ValueTask DisposeAsync()
    {
        _isClosed = true;
        return ValueTask.CompletedTask;
    }

    #region ReceiveMessagesAsync

    public override async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
    {
        return await GetStore().ReceiveAsync(maxMessages, maxWaitTime ?? _defaultMaxWaitTime, ReceiveMode, cancellationToken);
    }

    public override Task<ServiceBusReceivedMessage?> ReceiveMessageAsync(TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
    {
        return ServiceBusClientUtils.ReceiveSingleAsync(GetStore(), maxWaitTime ?? _defaultMaxWaitTime, ReceiveMode, cancellationToken);
    }

    public override IAsyncEnumerable<ServiceBusReceivedMessage> ReceiveMessagesAsync(CancellationToken cancellationToken = default)
    {
        return ServiceBusClientUtils.ReceiveAsAsyncEnumerable(GetStore(), ReceiveMode, cancellationToken);
    }

    #endregion

    #region Abandon, Complete, Renew Message

    public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
    {
        if (propertiesToModify is not null)
        {
            throw ServiceBusClientExceptionFactory.FeatureNotSupported("Properties cannot be modified.");
        }

        GetStore().AbandonMessage(message);
        return Task.CompletedTask;
    }

    public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
    {
        if (!GetStore().CompleteMessage(message))
        {
            throw ServiceBusClientExceptionFactory.MessageLockLost(FullyQualifiedNamespace, EntityPath);
        }

        return Task.CompletedTask;
    }

    public override Task RenewMessageLockAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
    {
        if (!GetStore().RenewMessageLock(message))
        {
            throw ServiceBusClientExceptionFactory.MessageLockLost(FullyQualifiedNamespace, EntityPath);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region Unsupported 

    public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object> propertiesToModify, string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeferMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<ServiceBusReceivedMessage> ReceiveDeferredMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveDeferredMessagesAsync(IEnumerable<long> sequenceNumbers, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    #endregion
}
