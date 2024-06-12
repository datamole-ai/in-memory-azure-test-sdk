using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.EventHubs.Faults;

namespace Datamole.InMemory.Azure.EventHubs.Clients;

public class InMemoryEventHubConsumerClient : EventHubConsumerClient
{
    public InMemoryEventHubProvider Provider { get; }

    #region Constructors

    public InMemoryEventHubConsumerClient(
        string consumerGroup,
        string namespaceHostname,
        string eventHubName,
        InMemoryEventHubProvider provider)
        : this(consumerGroup, EventHubClientUtils.GenerateConnection(namespaceHostname, eventHubName), provider) { }


    public InMemoryEventHubConsumerClient(
        string consumerGroup,
        EventHubConnection connection,
        InMemoryEventHubProvider provider)
        : base(consumerGroup, connection)
    {
        Provider = provider;
    }

    public static InMemoryEventHubConsumerClient FromEventHub(string consumerGroup, InMemoryEventHub eventHub)
    {
        return FromNamespace(consumerGroup, eventHub.Namespace, eventHub.Name);
    }

    public static InMemoryEventHubConsumerClient FromNamespace(string consumerGroup, InMemoryEventHubNamespace eventHubNamespace, string eventHubName)
    {
        return new(consumerGroup, eventHubNamespace.Hostname, eventHubName, eventHubNamespace.Provider);
    }

    #endregion

    private InMemoryEventHub GetEventHub()
    {
        var eventHub = EventHubClientUtils.GetEventHub(Provider, FullyQualifiedNamespace, EventHubName);

        EventHubClientUtils.HasConsumerGroupOrThrow(eventHub, ConsumerGroup);

        return eventHub;
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    private void CheckFaults(EventHubOperation operation)
    {
        var scope = new EventHubFaultScope
        {
            NamespaceName = InMemoryEventHubNamespace.GetNamespaceNameFromHostname(FullyQualifiedNamespace, Provider),
            EventHubName = EventHubName,
            ConsumerGroupName = ConsumerGroup,
            Operation = operation
        };

        EventHubClientUtils.CheckFaults(scope, Provider);
    }

    #region Get Properties & IDs

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
        var result = eventHub.GetPartition(partitionId).GetProperties();
        return Task.FromResult(result);
    }

    #endregion




}
