using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Clients.Internals;

namespace Datamole.InMemory.Azure.ServiceBus.Clients;

public class InMemoryServiceBusClient(string fullyQualifiedNamespace, ServiceBusClientOptions options, InMemoryServiceBusProvider provider) : ServiceBusClient
{
    private readonly InMemoryServiceBusProvider _provider = provider;
    private readonly TimeSpan _defaultMaxWaitTime = options.RetryOptions.MaxDelay;

    private bool _isClosed;

    public InMemoryServiceBusClient(string fullyQualifiedNamespace, InMemoryServiceBusProvider provider) : this(fullyQualifiedNamespace, new(), provider) { }

    public override string FullyQualifiedNamespace { get; } = fullyQualifiedNamespace;

    public override bool IsClosed => _isClosed;

    public override string Identifier { get; } = options.Identifier ?? Guid.NewGuid().ToString();

    public static InMemoryServiceBusClient FromNamespace(InMemoryServiceBusNamespace serviceBusNamespace, ServiceBusClientOptions? options = null)
    {
        return new(serviceBusNamespace.FullyQualifiedNamespace, options ?? new(), serviceBusNamespace.Provider);
    }

    public override ValueTask DisposeAsync()
    {
        _isClosed = true;
        return ValueTask.CompletedTask;
    }

    #region CreateSender

    public override ServiceBusSender CreateSender(string queueOrTopicName) => CreateSender(queueOrTopicName, new());

    public override ServiceBusSender CreateSender(string queueOrTopicName, ServiceBusSenderOptions options)
    {
        return new InMemoryServiceBusSender(FullyQualifiedNamespace, queueOrTopicName, options, _provider);
    }

    #endregion

    #region CreateReceiver

    public override ServiceBusReceiver CreateReceiver(string queueName) => CreateReceiver(queueName, new ServiceBusReceiverOptions());

    public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options)
    {
        return new InMemoryServiceBusReceiver(FullyQualifiedNamespace, queueName, options, _defaultMaxWaitTime, _provider);
    }

    public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName) => CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions());

    public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions options)
    {
        return new InMemoryServiceBusReceiver(FullyQualifiedNamespace, topicName, subscriptionName, options, _defaultMaxWaitTime, _provider);
    }

    #endregion

    #region AcceptNextSession

    public override async Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
    {
        var queue = ServiceBusClientUtils.GetQueue(FullyQualifiedNamespace, queueName, _provider);
        var session = await queue.MessageStore.TryAcquireNextAvailableSessionAsync(_defaultMaxWaitTime, cancellationToken);

        if (session is null)
        {
            throw ServiceBusClientExceptionFactory.NoSessionAvailable(FullyQualifiedNamespace, queueName);
        }

        return new InMemoryServiceBusSessionReceiver(session, options ?? new(), _defaultMaxWaitTime);
    }

    public override async Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string topicName, string subscriptionName, ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
    {
        var subscription = ServiceBusClientUtils.GetSubscription(FullyQualifiedNamespace, topicName, subscriptionName, _provider);

        var session = await subscription.MessageStore.TryAcquireNextAvailableSessionAsync(_defaultMaxWaitTime, cancellationToken);

        if (session is null)
        {
            throw ServiceBusClientExceptionFactory.NoSessionAvailable(FullyQualifiedNamespace, topicName, subscriptionName);
        }

        return new InMemoryServiceBusSessionReceiver(session, options ?? new(), _defaultMaxWaitTime);
    }

    #endregion

    #region AcceptSession
    public override Task<ServiceBusSessionReceiver> AcceptSessionAsync(string queueName, string sessionId, ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
    {
        var queue = ServiceBusClientUtils.GetQueue(FullyQualifiedNamespace, queueName, _provider);
        var session = queue.MessageStore.TryAcquireSession(sessionId);

        if (session is null)
        {
            throw ServiceBusClientExceptionFactory.SessionNotFound(FullyQualifiedNamespace, queueName, sessionId);
        }

        var receiver = new InMemoryServiceBusSessionReceiver(session, options ?? new(), _defaultMaxWaitTime);

        return Task.FromResult<ServiceBusSessionReceiver>(receiver);
    }

    public override Task<ServiceBusSessionReceiver> AcceptSessionAsync(string topicName, string subscriptionName, string sessionId, ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
    {
        var subscription = ServiceBusClientUtils.GetSubscription(FullyQualifiedNamespace, topicName, subscriptionName, _provider);
        var session = subscription.MessageStore.TryAcquireSession(sessionId);

        if (session is null)
        {
            throw ServiceBusClientExceptionFactory.SessionNotFound(FullyQualifiedNamespace, topicName, subscriptionName, sessionId);
        }

        var receiver = new InMemoryServiceBusSessionReceiver(session, options ?? new(), _defaultMaxWaitTime);

        return Task.FromResult<ServiceBusSessionReceiver>(receiver);
    }


    #endregion

    #region Unsupported

    public override ServiceBusProcessor CreateProcessor(string queueName) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusProcessor CreateProcessor(string topicName, string subscriptionName) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusProcessor CreateProcessor(string topicName, string subscriptionName, ServiceBusProcessorOptions options) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusSessionProcessor CreateSessionProcessor(string queueName, ServiceBusSessionProcessorOptions? options = null) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusSessionProcessor CreateSessionProcessor(string topicName, string subscriptionName, ServiceBusSessionProcessorOptions? options = null) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override ServiceBusRuleManager CreateRuleManager(string topicName, string subscriptionName) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    #endregion
}
