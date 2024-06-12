namespace Datamole.InMemory.Azure.EventHubs.Faults;

public record EventHubFaultScope
{
    public static EventHubFaultScope Any { get; } = new();

    public string? NamespaceName { get; init; }
    public string? EventHubName { get; init; }
    public string? ConsumerGroupName { get; init; }
    public string? PartitionId { get; init; }
    public EventHubOperation? Operation { get; init; }

    internal bool IsSubscopeOf(EventHubFaultScope other)
    {
        var result = true;

        result &= other.NamespaceName is null || other.NamespaceName == NamespaceName;
        result &= other.EventHubName is null || other.EventHubName == EventHubName;
        result &= other.ConsumerGroupName is null || other.ConsumerGroupName == ConsumerGroupName;
        result &= other.PartitionId is null || other.PartitionId == PartitionId;
        result &= other.Operation is null || other.Operation == Operation;

        return result;

    }
}
