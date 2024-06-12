using Azure;
using Azure.Storage.Blobs.Models;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Blobs.Clients;

namespace Tests.Storage.Blobs;

[TestClass]
public class BlobContainerClientTests
{
    [TestMethod]
    public async Task Operation_ForNonExistentContainer_ShouldFailAndThenSucceeed()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        var blobClient = containerClient.GetBlobClient("test-blob");

        var content = BinaryData.FromString("test");

        try
        {
            await blobClient.UploadAsync(content);
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode == BlobErrorCode.ContainerNotFound)
        {
            await containerClient.CreateIfNotExistsAsync();
            await blobClient.UploadAsync(content);
        }

        var containerExists = await containerClient.ExistsAsync();

        containerExists.Value.Should().BeTrue();

        var blobs = containerClient.GetBlobs();

        var blob = blobs.Should().ContainSingle(blob => blob.Name == "test-blob").Which;

        containerClient.GetBlobClient(blob.Name).DownloadContent().Value.Content.ToString().Should().Be("test");

    }




}
