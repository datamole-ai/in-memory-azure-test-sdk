namespace Datamole.InMemory.Azure.EventHubs.Faults;

public enum EventHubOperation
{
    Send,
    GetProperties,
    GetPartitionIds,
    GetPartitionProperties,
    ReceiveBatch
}
