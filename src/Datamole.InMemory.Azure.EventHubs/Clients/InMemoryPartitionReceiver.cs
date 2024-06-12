using System.Diagnostics;
using System.Globalization;
using System.Reflection;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.EventHubs.Faults;
using Datamole.InMemory.Azure.EventHubs.Internals;

namespace Datamole.InMemory.Azure.EventHubs.Clients;

public class InMemoryPartitionReceiver : PartitionReceiver
{
    private readonly SemaphoreSlim _receiveLock = new(1, 1);
    private readonly object _lastEnqueuedEventPropertiesLock = new();


    private readonly StartingPosition _startingPosition;

    private Position? _position;

    private LastEnqueuedEventProperties? _lastEnqueuedEventProperties;

    #region Constructors

    public InMemoryPartitionReceiver(
        string consumerGroup,
        string partitionId,
        string namespaceHostname,
        string eventHubName,
        EventPosition startingPosition,
        InMemoryEventHubProvider provider)
        : this(consumerGroup, partitionId, EventHubClientUtils.GenerateConnection(namespaceHostname, eventHubName), startingPosition, provider) { }


    public InMemoryPartitionReceiver(
        string consumerGroup,
        string partitionId,
        EventHubConnection connection,
        EventPosition startingPosition,
        InMemoryEventHubProvider provider)
        : base(consumerGroup, partitionId, startingPosition, connection)
    {
        Provider = provider;
        _startingPosition = ResolveStartingPosition(startingPosition);
    }

    public static InMemoryPartitionReceiver FromEventHub(string partitionId, InMemoryEventHub eventHub)
    {
        return FromEventHub(InMemoryEventHub.DefaultConsumerGroupName, partitionId, eventHub, EventPosition.Earliest);
    }

    public static InMemoryPartitionReceiver FromEventHub(string consumerGroup, string partitionId, InMemoryEventHub eventHub, EventPosition startingPosition)
    {
        return FromNamespace(consumerGroup, partitionId, eventHub.Namespace, eventHub.Name, startingPosition);
    }

    public static InMemoryPartitionReceiver FromNamespace(string consumerGroup, string partitionId, InMemoryEventHubNamespace eventHubNamespace, string eventHubName, EventPosition startingPosition)
    {
        return new(consumerGroup, partitionId, eventHubNamespace.Hostname, eventHubName, startingPosition, eventHubNamespace.Provider);
    }

    #endregion

    public InMemoryEventHubProvider Provider { get; }

    private InMemoryPartition GetPartition()
    {
        var eh = EventHubClientUtils.GetEventHub(Provider, FullyQualifiedNamespace, EventHubName);

        EventHubClientUtils.HasConsumerGroupOrThrow(eh, ConsumerGroup);

        if (!eh.TryGetPartition(PartitionId, out var partition))
        {
            throw EventHubClientExceptionFactory.PartitionNotFound(eh, PartitionId);
        }

        return partition;

    }

    private void CheckFaults(EventHubOperation operation)
    {
        var scope = new EventHubFaultScope
        {
            NamespaceName = InMemoryEventHubNamespace.GetNamespaceNameFromHostname(FullyQualifiedNamespace, Provider),
            EventHubName = EventHubName,
            ConsumerGroupName = ConsumerGroup,
            PartitionId = PartitionId,
            Operation = operation
        };

        EventHubClientUtils.CheckFaults(scope, Provider);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public override LastEnqueuedEventProperties ReadLastEnqueuedEventProperties()
    {
        lock (_lastEnqueuedEventPropertiesLock)
        {
            if (_lastEnqueuedEventProperties is null)
            {
                return default;
            }

            return _lastEnqueuedEventProperties.Value;
        }

    }

    #region Get Properties

    public override Task<PartitionProperties> GetPartitionPropertiesAsync(CancellationToken cancellationToken = default)
    {
        CheckFaults(EventHubOperation.GetProperties);

        var partition = GetPartition();

        var properties = partition.GetProperties();

        return Task.FromResult(properties);
    }

    #endregion

    #region Receive Batch

    public override async Task<IEnumerable<EventData>> ReceiveBatchAsync(int maximumEventCount, TimeSpan maximumWaitTime, CancellationToken cancellationToken = default)
    {
        CheckFaults(EventHubOperation.ReceiveBatch);

        var partition = GetPartition();

        var startTime = Stopwatch.GetTimestamp();

        IReadOnlyList<EventData> events = Array.Empty<EventData>();

        await _receiveLock.WaitAsync(cancellationToken);

        try
        {
            if (_position is null)
            {
                _position = partition.ResolvePosition(_startingPosition);
            }


            while (!cancellationToken.IsCancellationRequested)
            {
                events = partition.GetEvents(_position.Value, maximumEventCount);

                if (events.Count > 0 || Stopwatch.GetElapsedTime(startTime) > maximumWaitTime)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }

            var partitionProperties = partition.GetProperties();

            if (events.Count > 0)
            {
                _position = Position.FromSequenceNumber(events[^1].SequenceNumber, false);
            }

            lock (_lastEnqueuedEventPropertiesLock)
            {
                _lastEnqueuedEventProperties = new(
                    partitionProperties.LastEnqueuedSequenceNumber,
                    partitionProperties.LastEnqueuedOffset,
                    partitionProperties.LastEnqueuedTime,
                    DateTimeOffset.UtcNow);
            }
        }
        finally
        {
            _receiveLock.Release();
        }

        return events;
    }

    public override Task<IEnumerable<EventData>> ReceiveBatchAsync(int maximumEventCount, CancellationToken cancellationToken = default)
    {
        return ReceiveBatchAsync(maximumEventCount, TimeSpan.FromSeconds(60), cancellationToken);
    }

    #endregion

    private static StartingPosition ResolveStartingPosition(EventPosition position)
    {
        if (position == EventPosition.Earliest)
        {
            return StartingPosition.Earliest;
        }

        if (position == EventPosition.Latest)
        {
            return StartingPosition.Latest;
        }

        long? sequencenceNumber = null;
        bool? isInclusive = null;

        foreach (var property in position.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (property.Name == "SequenceNumber")
            {
                var sequencenceNumberObj = property.GetValue(position);

                sequencenceNumber = sequencenceNumberObj switch
                {
                    long l => l,
                    null => null,
                    string s => long.Parse(s, CultureInfo.InvariantCulture),
                    _ => throw new InvalidOperationException($"SequenceNumber property with value '{sequencenceNumberObj}' has unexpected type: {sequencenceNumberObj?.GetType()}.")
                };
            }

            if (property.Name == "IsInclusive")
            {
                isInclusive = (bool?) property.GetValue(position);
            }

            if (property.Name == "Offset" && property.GetValue(position) is not null)
            {
                throw new NotSupportedException("EventPosition with offset is not supported.");
            }
        }

        if (sequencenceNumber is null)
        {
            throw new InvalidOperationException("SequenceNumber property not available.");
        }

        if (isInclusive is null)
        {
            throw new InvalidOperationException("IsInclusive property not available.");
        }


        return StartingPosition.FromSequenceNumber(sequencenceNumber.Value, isInclusive.Value);
    }

}
