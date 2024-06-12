using System.Collections.Concurrent;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.EventHubs.Faults;
using Datamole.InMemory.Azure.EventHubs.Internals;

namespace Datamole.InMemory.Azure.EventHubs.Clients;

public class InMemoryEventHubProducerClient : EventHubProducerClient
{
    private readonly ConcurrentDictionary<EventDataBatch, (List<EventData> Events, SendEventOptions Options)> _batches
        = new(ReferenceEqualityComparer.Instance);

    #region Constructors

    public InMemoryEventHubProducerClient(string namespaceHostname, string eventHubName, InMemoryEventHubProvider provider)
        : this(EventHubClientUtils.GenerateConnection(namespaceHostname, eventHubName), provider) { }


    public InMemoryEventHubProducerClient(EventHubConnection connection, InMemoryEventHubProvider provider) : base(connection)
    {
        Provider = provider;
    }

    public InMemoryEventHubProducerClient(string connectionString, InMemoryEventHubProvider provider)
        : this(new EventHubConnection(connectionString), provider) { }


    public static InMemoryEventHubProducerClient FromEventHub(InMemoryEventHub eventHub)
    {
        return FromNamespace(eventHub.Namespace, eventHub.Name);
    }

    public static InMemoryEventHubProducerClient FromNamespace(InMemoryEventHubNamespace eventHubNamespace, string eventHubName)
    {
        return new(eventHubNamespace.Hostname, eventHubName, eventHubNamespace.Provider);
    }

    #endregion

    public InMemoryEventHubProvider Provider { get; }

    public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private InMemoryEventHub GetEventHub()
    {
        return EventHubClientUtils.GetEventHub(Provider, FullyQualifiedNamespace, EventHubName);
    }

    private void CheckFaults(EventHubOperation operation)
    {
        var scope = new EventHubFaultScope
        {
            NamespaceName = InMemoryEventHubNamespace.GetNamespaceNameFromHostname(FullyQualifiedNamespace, Provider),
            EventHubName = EventHubName,
            Operation = operation
        };

        EventHubClientUtils.CheckFaults(scope, Provider);
    }

    #region Create Batch

    public override ValueTask<EventDataBatch> CreateBatchAsync(CancellationToken cancellationToken = default)
    {
        return CreateBatchAsync(new CreateBatchOptions(), cancellationToken);
    }

    public override ValueTask<EventDataBatch> CreateBatchAsync(CreateBatchOptions options, CancellationToken cancellationToken = default)
    {
        var events = new List<EventData>();

        var batch = EventHubsModelFactory.EventDataBatch(42, events, options, ed => events.Count < 64);

        _batches[batch] = (events, options);

        return ValueTask.FromResult(batch);
    }

    #endregion

    #region  Get properties & IDs

    public override Task<EventHubProperties> GetEventHubPropertiesAsync(CancellationToken cancellationToken = default)
    {
        CheckFaults(EventHubOperation.GetProperties);

        var eventHub = GetEventHub();

        return Task.FromResult(eventHub.Properties);
    }

    public override Task<string[]> GetPartitionIdsAsync(CancellationToken cancellationToken = default)
    {
        CheckFaults(EventHubOperation.GetPartitionIds);

        var eventHub = GetEventHub();

        return Task.FromResult(eventHub.Properties.PartitionIds);
    }

    public override Task<PartitionProperties> GetPartitionPropertiesAsync(string partitionId, CancellationToken cancellationToken = default)
    {
        CheckFaults(EventHubOperation.GetPartitionProperties);

        var eventHub = GetEventHub();

        var properties = eventHub.GetPartition(partitionId).GetProperties();

        return Task.FromResult(properties);
    }

    protected override Task<PartitionPublishingProperties> GetPartitionPublishingPropertiesAsync(string partitionId, CancellationToken cancellationToken = default)
    {
        throw EventHubClientExceptionFactory.FeatureNotSupported();
    }

    #endregion

    #region Send

    private void SendCore(IEnumerable<EventData> eventBatch, SendEventOptions? sendEventOptions)
    {
        CheckFaults(EventHubOperation.Send);

        var partition = ResolvePartitionToSend(sendEventOptions);

        foreach (var e in eventBatch)
        {
            partition.SendEvent(e, sendEventOptions?.PartitionKey);
        }
    }

    public void Send(EventData eventData, SendEventOptions? sendEventOptions = null)
    {
        var batch = new[] { eventData };

        SendCore(batch, sendEventOptions);

    }

    public override Task SendAsync(IEnumerable<EventData> eventBatch, SendEventOptions sendEventOptions, CancellationToken cancellationToken = default)
    {
        SendCore(eventBatch, sendEventOptions);
        return Task.CompletedTask;
    }

    public override Task SendAsync(IEnumerable<EventData> eventBatch, CancellationToken cancellationToken = default)
    {
        SendCore(eventBatch, null);
        return Task.CompletedTask;
    }

    public override Task SendAsync(EventDataBatch eventBatch, CancellationToken cancellationToken = default)
    {
        if (!_batches.TryRemove(eventBatch, out var data))
        {
            throw EventHubClientExceptionFactory.FeatureNotSupported($"Batches from different instance of '{GetType()}' are not supported.");
        }

        var (events, options) = data;

        SendCore(events, options);
        return Task.CompletedTask;
    }

    private InMemoryPartition ResolvePartitionToSend(SendEventOptions? sendEventOptions)
    {
        var eventHub = GetEventHub();

        if (sendEventOptions?.PartitionId is not null)
        {
            if (!eventHub.TryGetPartition(sendEventOptions.PartitionId, out var partition))
            {
                throw EventHubClientExceptionFactory.PartitionNotFound(eventHub, sendEventOptions.PartitionId);
            }

            return partition;
        }

        if (sendEventOptions?.PartitionKey is not null)
        {
            return eventHub.GetPartitionByKey(sendEventOptions.PartitionKey);
        }

        return eventHub.GetRoundRobinPartition();
    }

    #endregion

}
