using System.Collections.Concurrent;

using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Clients.Internals;

namespace Datamole.InMemory.Azure.ServiceBus.Clients;
public class InMemoryServiceBusSender : ServiceBusSender
{
    private readonly ConcurrentDictionary<ServiceBusMessageBatch, List<ServiceBusMessage>> _batches = new(ReferenceEqualityComparer.Instance);
    private readonly InMemoryServiceBusProvider _provider;

    private bool _isClosed = false;

    internal InMemoryServiceBusSender(string fullyQualifiedNamespace, string entityPath, ServiceBusSenderOptions options, InMemoryServiceBusProvider provider)
    {
        Identifier = options?.Identifier ?? Guid.NewGuid().ToString();
        FullyQualifiedNamespace = fullyQualifiedNamespace;
        EntityPath = entityPath;
        _provider = provider;
    }

    public override string FullyQualifiedNamespace { get; }
    public override string EntityPath { get; }
    public override bool IsClosed => _isClosed;
    public override string Identifier { get; }

    public override async Task CloseAsync(CancellationToken cancellationToken = default) => await DisposeAsync();

    public override ValueTask DisposeAsync()
    {
        _isClosed = true;
        return ValueTask.CompletedTask;
    }

    #region Send

    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        var entity = ServiceBusClientUtils.GetEntity(FullyQualifiedNamespace, EntityPath, _provider);

        entity.AddMessage(message);

        return Task.CompletedTask;
    }

    public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default)
    {
        if (!_batches.TryRemove(messageBatch, out var messages))
        {
            var error = $"Batch can be sent only once and must be created from the same client instance. Current batch was already sent or originates from different client instance.";
            throw ServiceBusClientExceptionFactory.FeatureNotSupported(error);
        }

        var entity = ServiceBusClientUtils.GetEntity(FullyQualifiedNamespace, EntityPath, _provider);

        entity.AddMessages(messages);
        return Task.CompletedTask;
    }

    public override Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
    {
        var entity = ServiceBusClientUtils.GetEntity(FullyQualifiedNamespace, EntityPath, _provider);

        entity.AddMessages(messages.ToList());
        return Task.CompletedTask;
    }

    #endregion

    #region CreateMessageBatchAsync

    public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default)
    {
        var messages = new List<ServiceBusMessage>();
        var batch = ServiceBusModelFactory.ServiceBusMessageBatch(-1, messages);

        _batches[batch] = messages;

        return ValueTask.FromResult(batch);
    }

    public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CreateMessageBatchOptions options, CancellationToken cancellationToken = default)
    {
        return CreateMessageBatchAsync(cancellationToken);
    }

    #endregion

    #region Unsupported

    public override Task<long> ScheduleMessageAsync(ServiceBusMessage message, DateTimeOffset scheduledEnqueueTime, CancellationToken cancellationToken = default)
    {
        throw ServiceBusClientExceptionFactory.FeatureNotSupported();
    }

    public override Task<IReadOnlyList<long>> ScheduleMessagesAsync(IEnumerable<ServiceBusMessage> messages, DateTimeOffset scheduledEnqueueTime, CancellationToken cancellationToken = default)
    {
        throw ServiceBusClientExceptionFactory.FeatureNotSupported();
    }

    public override Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default)
    {
        throw ServiceBusClientExceptionFactory.FeatureNotSupported();
    }

    public override Task CancelScheduledMessagesAsync(IEnumerable<long> sequenceNumbers, CancellationToken cancellationToken = default)
    {
        throw ServiceBusClientExceptionFactory.FeatureNotSupported();
    }
    #endregion

}
