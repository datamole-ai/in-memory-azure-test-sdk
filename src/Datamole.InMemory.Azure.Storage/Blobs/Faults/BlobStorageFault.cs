using Azure;

using Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;
using Datamole.InMemory.Azure.Storage.Faults;

namespace Datamole.InMemory.Azure.Storage.Blobs.Faults;

public abstract record BlobStorageFault(BlobStorageFaultScope Scope) : StorageFault<BlobStorageFaultScope>(Scope)
{
    internal record ServiceIsBusy(BlobStorageFaultScope Scope) : BlobStorageFault(Scope)
    {
        public override RequestFailedException CreateException(BlobStorageFaultScope currentScope)
        {
            return BlobClientExceptionFactory.ServiceIsBusy(currentScope);
        }
    }
}
