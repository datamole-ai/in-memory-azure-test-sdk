using Datamole.InMemory.Azure.Storage.Faults;

namespace Datamole.InMemory.Azure.Storage.Tables.Faults;

public record TableStorageFaultScope
{
    public string? StorageAccountName { get; init; }
    public string? TableName { get; init; }
    public TableOperation? Operation { get; init; }



    internal bool IsSubscopeOf(TableStorageFaultScope other)
    {
        var result = true;

        result &= other.StorageAccountName is null || other.StorageAccountName == StorageAccountName;
        result &= other.TableName is null || other.TableName == TableName;
        result &= other.Operation is null || other.Operation == Operation;

        return result;
    }

    public static TableStorageFaultScope FromStorageScope(StorageFaultScope scope)
    {
        return new() { StorageAccountName = scope.StorageAccountName };
    }
}
