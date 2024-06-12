using System.Diagnostics.CodeAnalysis;

using Azure;
using Azure.Storage.Blobs.Models;

namespace Datamole.InMemory.Azure.Storage.Blobs.Internals;

internal class InMemoryBlobContainer
{

    private readonly object _lock = new();
    private readonly Dictionary<string, InMemoryBlockBlob> _blobs = new();
    private readonly BlobContainerProperties _properties;
    public InMemoryBlobContainer(string name, IDictionary<string, string>? metadata, InMemoryBlobService service)
    {
        Name = name;
        Service = service;
        _properties = BlobsModelFactory.BlobContainerProperties(
            lastModified: DateTimeOffset.UtcNow,
            eTag: new ETag(Guid.NewGuid().ToString()),
            metadata: metadata);
    }

    public string Name { get; }
    public BlobContainerProperties GetProperties()
    {
        lock (_lock)
        {
            return _properties;
        }
    }

    public InMemoryBlobService Service { get; }

    public override string? ToString() => $"{Service} / {Name}";

    public IReadOnlyList<InMemoryBlockBlob> GetBlobs()
    {
        lock (_lock)
        {
            return _blobs.Values.ToList();
        }
    }

    public bool TryGetBlob(string blobName, [NotNullWhen(true)] out InMemoryBlockBlob? blob)
    {
        lock (_lock)
        {
            return _blobs.TryGetValue(blobName, out blob);
        }
    }

    public bool GetOrAddBlockBlob(string blobName, out InMemoryBlockBlob result)
    {
        lock (_lock)
        {
            if (_blobs.TryGetValue(blobName, out var existingBlob))
            {
                result = existingBlob;
                return false;
            }

            result = new(blobName, this);
            _blobs.Add(blobName, result);
            return true;
        }

    }

}

