using Azure;
using Azure.Storage.Blobs.Specialized;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Blobs.Clients;
using Datamole.InMemory.Azure.Storage.Blobs.Faults;

namespace Tests.Storage.Blobs;

[TestClass]
public class FaultInjectionTests
{

    [TestMethod]
    public void InjectedTransientFault_ScopedToOperation_ShouldBeResolved()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var fault = account.InjectFault(faults => faults
            .ForBlobService()
            .ServiceIsBusy()
            .WithScope(scope => scope with { Operation = BlobOperation.BlobCommitBlockList })
            .WithTransientOccurrences(2));

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        var stage = () => blobClient.StageBlock("id1", BinaryData.FromString("test").ToStream());
        var commit = () => blobClient.CommitBlockList(new[] { "id1" });

        stage.Should().NotThrow();

        commit.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");

        commit.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");


        commit.Should().NotThrow();

    }

    [TestMethod]
    public void InjectedPersistentFault_ScopedToOperation_ShouldBeResolved()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var containerClient = InMemoryBlobContainerClient.FromAccount(account, "test-container");

        containerClient.Create();

        var fault = account.InjectFault(faults => faults
            .ForBlobService()
            .ServiceIsBusy()
            .WithScope(scope => scope with { Operation = BlobOperation.BlobCommitBlockList }));

        var blobClient = containerClient.GetBlockBlobClient("test-blob");

        var stage = () => blobClient.StageBlock("id1", BinaryData.FromString("test").ToStream());
        var commit = () => blobClient.CommitBlockList(new[] { "id1" });

        stage.Should().NotThrow();

        commit.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");

        fault.Resolve();

        commit.Should().NotThrow();

    }

}
