using Azure.Storage.Blobs;

using Datamole.InMemory.Azure.Storage.Blobs.Faults;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;
public static class BlobClientUtils
{
    public static BlobUriBuilder GetUriBuilder(Uri uri, string? blobContainerName = null, string? blobName = null)
    {
        var builder = new BlobUriBuilder(uri);

        if (blobContainerName is not null)
        {
            builder.BlobContainerName = blobContainerName;
        }

        if (blobName is not null)
        {
            builder.BlobName = blobName;
        }

        return builder;
    }

    public static void CheckFaults(BlobStorageFaultScope currentScope, InMemoryStorageProvider provider)
    {
        if (!provider.Faults.TryGetFault<BlobStorageFault>(f => currentScope.IsSubscopeOf(f.Scope), out var fault))
        {
            return;
        }

        throw fault.CreateException(currentScope);
    }
}
