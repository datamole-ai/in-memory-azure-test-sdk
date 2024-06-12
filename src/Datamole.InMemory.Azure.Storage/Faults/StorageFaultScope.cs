namespace Datamole.InMemory.Azure.Storage.Faults;

public record StorageFaultScope
{
    public string? StorageAccountName { get; init; }
}

