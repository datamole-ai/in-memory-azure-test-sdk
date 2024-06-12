using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Blobs.Clients;

namespace Tests.Storage.Blobs;

[TestClass]
public class BlockBlobClientTests
{
    [TestMethod]
    public void StageBlock_AndCommitBlockList_ShouldCreateBlobWithCommitedBlocks()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobclient = containerClient.GetBlockBlobClient("test-blob");

        blobclient.StageBlock("test-block-id", BinaryData.FromString("test-data").ToStream());

        blobclient.CommitBlockList(new[] { "test-block-id" });

        var blockList = blobclient.GetBlockList().Value;

        blockList.CommittedBlocks.Should().ContainSingle(block => block.Name == "test-block-id");
        blockList.UncommittedBlocks.Should().BeEmpty();

        blobclient.DownloadContent().Value.Content.ToString().Should().Be("test-data");

    }

    [TestMethod]
    public void StageBlock_AndCommitEmptyBlockList_ShouldCreateBlobAndClearUncommitedBlocks()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        blobClient.StageBlock("test-block-id", BinaryData.FromString("test-data").ToStream());

        blobClient.GetBlockList().Value.UncommittedBlocks.Should().ContainSingle(b => b.Name == "test-block-id");

        blobClient.CommitBlockList(Enumerable.Empty<string>());

        var blockList = blobClient.GetBlockList().Value;

        blockList.CommittedBlocks.Should().BeEmpty();
        blockList.UncommittedBlocks.Should().BeEmpty();

    }

    [TestMethod]
    public void EmptyCommitBlockList_ShouldCreateEmptyBlob()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        blobClient.CommitBlockList(Enumerable.Empty<string>());

        var blockList = blobClient.GetBlockList().Value;

        blockList.CommittedBlocks.Should().BeEmpty();
        blockList.UncommittedBlocks.Should().BeEmpty();

    }

    [TestMethod]
    public void PropertiesAndHeaders_ShouldBeSetDuringBlockListCommit()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        blobClient.StageBlock("test-block-id", BinaryData.FromString("test-data").ToStream());

        blobClient.CommitBlockList(
            new[] { "test-block-id" },
            new BlobHttpHeaders
            {
                ContentType = "test/test",
                ContentEncoding = "gzip"
            },
            new Dictionary<string, string> { { "metadata1", "42" } }
            );


        var blockList = blobClient.GetBlockList().Value;

        blockList.CommittedBlocks.Should().ContainSingle(block => block.Name == "test-block-id");
        blockList.UncommittedBlocks.Should().BeEmpty();

        var properties = blobClient.GetProperties().Value;

        properties.Metadata.Should().Contain("metadata1", "42");
        properties.ContentType.Should().Be("test/test");
        properties.ContentEncoding.Should().Be("gzip");

    }

    [TestMethod]
    public void BlobWithUncommitedBlocksOnly_ShouldNotExistButBlockListReturned()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        blobClient.StageBlock("test-block-id", BinaryData.FromString("test-data").ToStream());

        blobClient.Exists().Value.Should().BeFalse();

        var blockList = blobClient.GetBlockList().Value;

        blockList.CommittedBlocks.Should().BeEmpty();
        blockList.UncommittedBlocks.Should().ContainSingle(b => b.Name == "test-block-id");

    }

    [TestMethod]
    public void BlobWithUncommitedBlocksOnly_ShouldNotCauseOverwrite()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var blockBlobClient = containerClient.GetBlockBlobClient("test-blob");

        blockBlobClient.StageBlock("test-block-id", BinaryData.FromString("test-data-1").ToStream());

        var blobClient = containerClient.GetBlobClient("test-blob");

        blobClient.Upload(BinaryData.FromString("test-data-2"), overwrite: false);

        blobClient.DownloadContent().Value.Content.ToString().Should().Be("test-data-2");


    }
}
