using Azure;
using Azure.Storage.Blobs.Specialized;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Blobs.Clients;
using Datamole.InMemory.Azure.Storage.FluentAssertions;

namespace Tests.Storage.Blobs;

[TestClass]
public class BlobClientTests
{
    [TestMethod]
    public void ParentContainer_ShouldBeReturned()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = InMemoryBlobClient.FromAccount(account, "test-container", "test-blob");

        blobClient.Upload(BinaryData.FromString("test-data"));

        var containerClientFromBlob = blobClient.GetParentBlobContainerClient();

        var blobs = containerClientFromBlob.GetBlobs().ToList();

        blobs.Should().ContainSingle(blob => blob.Name == "test-blob");

    }

    [TestMethod]
    public void ConstructedFromSasUri_ShouldBeReadable()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlobClient("test-blob");

        blobClient.Upload(BinaryData.FromString("test-data"));

        var blobClientFromUri = InMemoryBlobClient.FromAccount(account, "test-container", "test-blob");

        var content = blobClientFromUri.DownloadContent();

        content.Value.Content.ToString().Should().Be("test-data");

    }

    [TestMethod]
    public void Upload_ForExistingBlob_ShouldFail()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlobClient("test-blob");

        var act = () => blobClient.Upload(BinaryData.FromString("test-data"), overwrite: false);

        act.Should().NotThrow();
        act.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ConditionNotMet");

    }

    [TestMethod]
    public async Task BlobExistence_ShouldBeAwaited()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var blob = InMemoryBlobClient.FromAccount(account, "test-container", "test-blob");

        blob.GetParentBlobContainerClient().Create();

        var existTask = blob.Should().ExistAsync();

        existTask.IsCompleted.Should().BeFalse();

        await Task.Delay(100);

        existTask.IsCompleted.Should().BeFalse();

        await Task.Delay(100);

        blob.Upload(BinaryData.FromString("test-data"));

        await existTask;

        existTask.IsCompletedSuccessfully.Should().BeTrue();

    }


}
