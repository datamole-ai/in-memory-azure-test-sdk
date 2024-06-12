using Datamole.InMemory.Azure.Storage.Blobs.Faults;
using Datamole.InMemory.Azure.Storage.Tables.Faults;

namespace Datamole.InMemory.Azure.Storage.Faults;

public record StorageProviderFaultBuilder(StorageFaultScope Scope)
{
    public BlobStorageFaultBuilder ForBlobService() => new(BlobStorageFaultScope.FromStorageScope(Scope));

    public TableStorageFaultBuilder ForTableService() => new(TableStorageFaultScope.FromStorageScope(Scope));
}


