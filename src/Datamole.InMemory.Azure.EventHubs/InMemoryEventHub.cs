using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.EventHubs.Faults;
using Datamole.InMemory.Azure.EventHubs.Internals;
using Datamole.InMemory.Azure.Faults;

namespace Datamole.InMemory.Azure.EventHubs;

public class InMemoryEventHub
{
    public const string DefaultConsumerGroupName = "$Default";

    private readonly IReadOnlyDictionary<string, InMemoryPartition> _partitions;
    private readonly object _roundRobinPartitionLock = new();
    private readonly ConcurrentDictionary<string, byte> _consumerGroups;
    private int _roundRobinPartitionIndex = 0;

    public InMemoryEventHub(
        string name,
        EventHubProperties properties,
        InMemoryEventHubOptions options,
        InMemoryEventHubNamespace @namespace)
    {
        Namespace = @namespace;
        Name = name;
        Properties = properties ?? throw new ArgumentNullException(nameof(properties));
        _consumerGroups = new(StringComparer.OrdinalIgnoreCase);
        _consumerGroups[DefaultConsumerGroupName] = 1;
        _partitions = CreatePartitions(Properties.PartitionIds, options, this);
    }

    public int GetInitialSequenceNumber(string partitionId)
    {
        if (!_partitions.TryGetValue(partitionId, out var partition))
        {
            throw ExceptionFactory.PartitionNotFound(partitionId, this);
        }

        return partition.InitialSequenceNumber;
    }

    private static IReadOnlyDictionary<string, InMemoryPartition> CreatePartitions(string[] partitionIds, InMemoryEventHubOptions options, InMemoryEventHub parent)
    {
        var result = new Dictionary<string, InMemoryPartition>();

        Random? random = null;

        if (options.RandomizeInitialSequenceNumbers)
        {
            random = options.RandomizationSeed is null ? Random.Shared : new(options.RandomizationSeed.Value);
        }

        foreach (var id in partitionIds)
        {
            var initialSequenceNumber = random is null ? 0 : random.Next(options.MinRandomInitialSequenceNumber, options.MaxRandomInitialSequenceNumber + 1);

            result[id] = new(id, initialSequenceNumber, parent);
        }

        return result;
    }

    public InMemoryEventHubProvider Provider => Namespace.Provider;
    public InMemoryEventHubNamespace Namespace { get; }
    public EventHubProperties Properties { get; }

    public IReadOnlySet<string> ConsumerGroups => _consumerGroups.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
    public string Name { get; }

    public bool HasConsumerGroup(string consumerGroupName) => _consumerGroups.ContainsKey(consumerGroupName);

    public string GetConnectionString() => EventHubClientUtils.CreateConnectionStringForEventHub(Namespace.Hostname, Name);

    public override string ToString() => $"{Name} [{Namespace}]";

    public InMemoryEventHub AddConsumerGroup(string consumerGroupName)
    {
        if (!_consumerGroups.TryAdd(consumerGroupName, 1))
        {
            throw ExceptionFactory.ConsumerGroupAlreadyExists(consumerGroupName, this);
        }

        return this;
    }

    public IFaultRegistration InjectFault(Func<EventHubFaultBuilder, Fault> faultAction)
    {
        return Namespace.InjectFault(builder => faultAction(builder), new() { EventHubName = Name });
    }

    internal InMemoryPartition GetRoundRobinPartition()
    {
        int index;

        lock (_roundRobinPartitionLock)
        {
            if (_roundRobinPartitionIndex == _partitions.Count)
            {
                index = _roundRobinPartitionIndex = 0;
            }
            else
            {
                index = _roundRobinPartitionIndex++;
            }
        }

        return GetPartition(PartitionIdFromInt(index));
    }

    internal InMemoryPartition GetPartition(string partitionId)
    {
        if (TryGetPartition(partitionId, out var partition))
        {
            return partition;
        }

        throw ExceptionFactory.PartitionNotFound(partitionId, this);
    }

    internal bool TryGetPartition(string partitionId, [NotNullWhen(true)] out InMemoryPartition? partition)
    {
        return _partitions.TryGetValue(partitionId, out partition);
    }

    internal InMemoryPartition GetPartitionByKey(string partitionKey)
    {
        var hashBytes = MD5.HashData(Encoding.UTF8.GetBytes(partitionKey));

        var hashCode = BinaryPrimitives.ReadInt32BigEndian(hashBytes.AsSpan(0, 4));

        hashCode -= int.MinValue;

        var partitionId = hashCode % Properties.PartitionIds.Length;

        return GetPartition(PartitionIdFromInt(partitionId));
    }

    private static string PartitionIdFromInt(int partitionId) => partitionId.ToString(CultureInfo.InvariantCulture);


}
