using System.Diagnostics.CodeAnalysis;

using Datamole.InMemory.Azure.Storage.Blobs.Clients;
using Datamole.InMemory.Azure.Storage.Blobs.Faults;

namespace Datamole.InMemory.Azure.Storage.Blobs.Internals;

internal class InMemoryBlobService
{
    public Dictionary<string, InMemoryBlobContainer> _containers = new();

    public InMemoryBlobService(InMemoryStorageAccount account)
    {
        Uri = CreateServiceUri(account.Name, account.Provider);
        Account = account;
    }

    public Uri Uri { get; }

    internal BlobStorageFaultScope FaultScope => new() { StorageAccountName = Account.Name };

    public static int MaxBlockCount { get; } = 50_000;
    public static int MaxUncommitedBlocks { get; } = 100_000;
    public static int MaxBlockSize { get; } = 2000 * 1024 * 1024;

    public InMemoryStorageAccount Account { get; }

    public bool TryAddBlobContainer(string blobContainerName, IDictionary<string, string>? metadata, out InMemoryBlobContainer result)
    {
        lock (_containers)
        {
            if (_containers.TryGetValue(blobContainerName, out var existingContainer))
            {
                result = existingContainer;
                return false;
            }

            var newContainer = new InMemoryBlobContainer(blobContainerName, metadata, this);

            _containers.Add(blobContainerName, newContainer);

            result = newContainer;

            return true;
        }

    }

    public bool TryGetBlobContainer(string blobContainerName, [NotNullWhen(true)] out InMemoryBlobContainer? container)
    {
        lock (_containers)
        {
            return _containers.TryGetValue(blobContainerName, out container);
        }
    }

    public static Uri CreateServiceUri(string accountName, InMemoryStorageProvider provider)
    {
        return new($"https://{accountName}.blob.{provider.HostnameSuffix}");
    }

    public InMemoryBlobServiceClient GetClient() => new(Uri, Account.Provider);

    public override string ToString() => Uri.ToString().TrimEnd('/');

    public bool ContainerExists(string name) => TryGetBlobContainer(name, out _);
}
