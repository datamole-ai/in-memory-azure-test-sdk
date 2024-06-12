using System.Diagnostics.CodeAnalysis;

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Datamole.InMemory.Azure.Storage.Blobs.Faults;
using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Internals;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;

internal class BlobClientCore
{
    public BlobClientCore(BlobUriBuilder uriBuilder, InMemoryStorageProvider provider)
    {
        Uri = uriBuilder.ToUri();
        AccountName = uriBuilder.AccountName;
        BlobContainerName = uriBuilder.BlobContainerName;
        Name = uriBuilder.BlobName;
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public Uri Uri { get; }
    public string AccountName { get; }
    public string BlobContainerName { get; }
    public string Name { get; }
    public InMemoryStorageProvider Provider { get; }



    public BlobDownloadInfo Download(BlobDownloadOptions? options)
    {
        var blob = DownloadCore(options);
        return blob.GetDownloadInfo();
    }

    public BlobDownloadStreamingResult DownloadStreaming(BlobDownloadOptions? options)
    {
        var blob = DownloadCore(options);
        return blob.GetStreamingDownloadResult();
    }

    public BlobDownloadResult DownloadContent(BlobDownloadOptions? options)
    {
        var blob = DownloadCore(options);
        return blob.GetDownloadResult();
    }

    private InMemoryBlockBlob DownloadCore(BlobDownloadOptions? options)
    {
        CheckFaults(BlobOperation.BlobDownload);

        var blob = GetBlob();

        CheckConditions(blob.GetProperties().ETag, options?.Conditions);

        return blob;
    }


    public BlobProperties GetProperties(BlobRequestConditions? conditions)
    {
        CheckFaults(BlobOperation.BlobGetProperties);

        var blob = GetBlob();

        var properties = blob.GetProperties();

        CheckConditions(properties.ETag, conditions);

        return properties;
    }

    public bool Exists()
    {
        CheckFaults(BlobOperation.BlobExists);

        if (!TryGetBlob(out var blob))
        {
            return false;
        }

        return blob.HasCommitedBlockList();

    }

    public BlockList GetBlockList(BlockListTypes types, BlobRequestConditions? conditions)
    {
        CheckFaults(BlobOperation.BlobGetBlockList);

        var blob = GetBlob();

        CheckConditions(blob.GetProperties().ETag, conditions);

        return blob.GetBlockList(types);
    }

    public BlobContentInfo Upload(BinaryData content, BlobUploadOptions? options)
    {
        CheckFaults(BlobOperation.BlobUpload);

        var container = GetContainer();

        var blobAdded = container.GetOrAddBlockBlob(Name, out var blob);

        var currentETag = ResolveCurrentETag(blobAdded, blob);

        CheckConditions(currentETag, options?.Conditions);

        var blockList = StageContentAsBlocks(content.ToMemory(), blob).ToList();

        return CommitBlockListCore(blockList, blob, options?.HttpHeaders, options?.Metadata);
    }



    public BlobContentInfo CommitBlockList(IEnumerable<string> blockIds, CommitBlockListOptions? options)
    {
        CheckFaults(BlobOperation.BlobCommitBlockList);

        var container = GetContainer();

        var blobAdded = container.GetOrAddBlockBlob(Name, out var blob);

        var currentETag = ResolveCurrentETag(blobAdded, blob);

        CheckConditions(currentETag, options?.Conditions);

        return CommitBlockListCore(blockIds, blob, options?.HttpHeaders, options?.Metadata);
    }


    public BlockInfo StageBlock(string blockId, BinaryData content, BlockBlobStageBlockOptions? options)
    {
        CheckFaults(BlobOperation.BlobStageBlock);

        var container = GetContainer();

        container.GetOrAddBlockBlob(Name, out var blob);

        CheckConditions(blob.GetProperties().ETag, options?.Conditions);

        if (!blob.TryStageBlock(blockId, content, out var block, out var error))
        {
            throw BlobClientExceptionFactory.StageBlockError(AccountName, BlobContainerName, Name, error);
        }

        return block.GetInfo();
    }

    public BlobContainerClient GetParentContainerClient()
    {
        var containerUriBuilder = new BlobUriBuilder(Uri)
        {
            BlobName = null
        };

        return new InMemoryBlobContainerClient(containerUriBuilder.ToUri(), Provider);
    }

    private static ETag? ResolveCurrentETag(bool blobAdded, InMemoryBlockBlob blob)
    {
        if (blobAdded)
        {
            return null;
        }

        if (!blob.HasCommitedBlockList())
        {
            return null;
        }

        return blob.GetProperties().ETag;
    }

    private BlobContentInfo CommitBlockListCore(IEnumerable<string> blockIds, InMemoryBlockBlob blob, BlobHttpHeaders? headers, IDictionary<string, string>? metadata)
    {
        if (!blob.TryCommitBlockList(blockIds, headers, metadata, out var error))
        {
            throw BlobClientExceptionFactory.ResolveCommitBlockListError(AccountName, BlobContainerName, Name, error);
        }

        return blob.GetContentInfo();
    }

    private IEnumerable<string> StageContentAsBlocks(ReadOnlyMemory<byte> content, InMemoryBlockBlob blob)
    {
        var index = 0;

        while (index < content.Length)
        {
            var blockSize = Math.Min(content.Length - index, InMemoryBlobService.MaxBlockSize);

            var blockId = Guid.NewGuid().ToString();

            var block = new BinaryData(content[index..blockSize]);

            if (!blob.TryStageBlock(blockId, block, out _, out var error))
            {
                throw BlobClientExceptionFactory.StageBlockError(AccountName, BlobContainerName, Name, error);
            }

            yield return blockId;

            index += blockSize;
        }
    }


    private void CheckConditions(ETag? currentETag, BlobRequestConditions? conditions)
    {
        if (!ConditionChecker.CheckConditions(currentETag, conditions?.IfMatch, conditions?.IfNoneMatch, out var error))
        {
            throw BlobClientExceptionFactory.ConditionNotMet(error.ConditionType, AccountName, BlobContainerName, Name, error.Message);
        }
    }

    private void CheckFaults(BlobOperation operation)
    {
        var currentScope = new BlobStorageFaultScope()
        {
            StorageAccountName = AccountName,
            BlobContainerName = BlobContainerName,
            BlobName = Name,
            Operation = operation
        };

        BlobClientUtils.CheckFaults(currentScope, Provider);
    }

    private InMemoryBlobContainer GetContainer()
    {
        if (!Provider.TryGetAccount(AccountName, out var account))
        {
            throw BlobClientExceptionFactory.BlobServiceNotFound(AccountName, Provider);
        }

        if (!account.BlobService.TryGetBlobContainer(BlobContainerName, out var container))
        {
            throw BlobClientExceptionFactory.ContainerNotFound(BlobContainerName, account.BlobService);
        }

        return container;
    }

    private InMemoryBlockBlob GetBlob()
    {
        if (!TryGetBlob(out var blob))
        {
            throw BlobClientExceptionFactory.BlobNotFound(AccountName, BlobContainerName, Name);
        }

        return blob;
    }

    private bool TryGetBlob([NotNullWhen(true)] out InMemoryBlockBlob? result)
    {
        var container = GetContainer();

        if (!container.TryGetBlob(Name, out var blob))
        {
            result = null;
            return false;
        }

        result = blob;
        return true;
    }


}
