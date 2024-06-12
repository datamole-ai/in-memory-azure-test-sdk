namespace Datamole.InMemory.Azure.Storage.Blobs.Faults;

public record BlobStorageFaultBuilder(BlobStorageFaultScope Scope)
{
    public BlobStorageFault ServiceIsBusy() => new BlobStorageFault.ServiceIsBusy(Scope);
}



