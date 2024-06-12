using Datamole.InMemory.Azure.Storage.Faults;

namespace Datamole.InMemory.Azure.Storage.Blobs.Faults;

public record BlobStorageFaultScope
{
    public string? StorageAccountName { get; init; }
    public string? BlobContainerName { get; init; }
    public string? BlobName { get; init; }
    public BlobOperation? Operation { get; init; }



    internal bool IsSubscopeOf(BlobStorageFaultScope other)
    {
        var result = true;

        result &= other.StorageAccountName is null || other.StorageAccountName == StorageAccountName;
        result &= other.BlobContainerName is null || other.BlobContainerName == BlobContainerName;
        result &= other.BlobName is null || other.BlobName == BlobName;
        result &= other.Operation is null || other.Operation == Operation;

        return result;

    }

    public static BlobStorageFaultScope FromStorageScope(StorageFaultScope scope)
    {
        return new() { StorageAccountName = scope.StorageAccountName };
    }
}

