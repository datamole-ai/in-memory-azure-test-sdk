using Azure.Storage.Blobs.Specialized;

namespace Datamole.InMemory.Azure.Storage.FluentAssertions;

public static class ShouldExtensions
{
    public static BlockBlobClientAssertions Should(this BlobBaseClient client)
    {
        return new(client);
    }
}
