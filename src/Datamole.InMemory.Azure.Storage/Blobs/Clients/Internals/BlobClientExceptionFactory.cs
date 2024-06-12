using Azure;
using Azure.Storage.Blobs.Models;

using Datamole.InMemory.Azure.Storage.Blobs.Faults;
using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Internals;

using CommitBlockListError = Datamole.InMemory.Azure.Storage.Blobs.Internals.InMemoryBlockBlob.CommitBlockListError;
using StageBlockError = Datamole.InMemory.Azure.Storage.Blobs.Internals.InMemoryBlockBlob.StageBlockError;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;

internal static class BlobClientExceptionFactory
{
    public static HttpRequestException BlobServiceNotFound(string accountName, InMemoryStorageProvider provider)
    {
        return new($"Host '{InMemoryBlobService.CreateServiceUri(accountName, provider)}' not found.");
    }



    public static RequestFailedException ContainerNotFound(string containerName, InMemoryBlobService blobService)
    {
        return new(
            404,
            $"Container '{containerName}' not found in '{blobService}' account.",
            BlobErrorCode.ContainerNotFound.ToString(),
            null);
    }

    public static RequestFailedException ContainerAlreadyExists(string accountName, string containerName)
    {
        return new(
          412,
          $"Container '{containerName}' in account '{accountName}' not foundk.",
          BlobErrorCode.ContainerNotFound.ToString(),
          null);
    }

    public static RequestFailedException BlobNotFound(string accountName, string blobContainerName, string blobName)
    {
        return new(404, $"Blob '{blobName}' not found in container '{blobContainerName}' in account '{accountName}'.", BlobErrorCode.BlobNotFound.ToString(), null);
    }

    public static RequestFailedException BlockCountExceeded(string accountName, string blobContainerName, string blobName, int limit, int actualCount)
    {
        return new(
            409,
            $"Number of blocks for in a block list ({actualCount} exceeded the limit ({limit}) " +
            $"in blob '{blobName}' in container '{blobContainerName}' in account '{accountName}'.",
            BlobErrorCode.BlockCountExceedsLimit.ToString(),
            null
            );
    }

    public static RequestFailedException BlockNotFound(string accountName, string blobContainerName, string blobName, string blockId)
    {
        return new(
            409,
            $"Block '{blockId}' not found in blob '{blobName}' in container '{blobContainerName}' in account '{accountName}'.",
            BlobErrorCode.InvalidBlockList.ToString(),
            null
            );
    }



    public static RequestFailedException ServiceIsBusy(BlobStorageFaultScope scope)
    {
        return new(
            503,
            $"Blob service in account '{scope.StorageAccountName}' is busy.",
            BlobErrorCode.ServerBusy.ToString(),
            null);
    }

    public static RequestFailedException TooManyUncommittedBlocks(string accountName, string blobContainerName, string blobName, int limit, int actualCount)
    {
        return new(
            409,
            $"Number of uncommited blocks ({actualCount}) exceeded the limit ({limit}) " +
            $"in blob '{blobName}' in container '{blobContainerName}' in account '{accountName}'.",
            BlobErrorCode.BlockCountExceedsLimit.ToString(),
            null);
    }

    public static RequestFailedException BlockTooLarge(string accountName, string blobContainerName, string blobName, int limit, int actualSize)
    {
        return new(
            413,
            $"Size of block ({actualSize}) exceeded the limit ({limit}) " +
            $"in blob '{blobName}' in container '{blobContainerName}' in account '{accountName}'.",
            BlobErrorCode.RequestBodyTooLarge.ToString(),
            null);
    }

    public static RequestFailedException StageBlockError(string accountName, string blobContainerName, string blobName, StageBlockError error)
    {
        return error switch
        {
            StageBlockError.BlockTooLarge blockTooLarge
                => BlockTooLarge(accountName, blobContainerName, blobName, blockTooLarge.Limit, blockTooLarge.ActualSize),
            StageBlockError.TooManyUncommittedBlocks tooManyUncommittedBlocks
                => TooManyUncommittedBlocks(accountName, blobContainerName, blobName, tooManyUncommittedBlocks.Limit, tooManyUncommittedBlocks.ActualCount),
            _ => throw new InvalidOperationException($"Unexpected error: {error}.")
        };
    }

    public static RequestFailedException ResolveCommitBlockListError(string accountName, string blobContainerName, string blobName, CommitBlockListError error)
    {
        return error switch
        {
            CommitBlockListError.BlockCountExceeded blockCountExceeded
                => BlockCountExceeded(accountName, blobContainerName, blobName, blockCountExceeded.Limit, blockCountExceeded.ActualCount),
            CommitBlockListError.BlockNotFound blockNotFound
                => BlockNotFound(accountName, blobContainerName, blobName, blockNotFound.BlockId),
            _ => throw new InvalidOperationException($"Unexpected error: {error}."),
        };
    }

    public static RequestFailedException ConditionNotMet(ConditionType conditionType, string accountName, string blobContainerName, string blobName, string message)
    {
        return new(
            412,
            $"Condition {conditionType} " +
            $"for blob '{blobName}' in container '{blobContainerName}' in account '{accountName} " +
            $"not met: {message}'.",
            BlobErrorCode.ConditionNotMet.ToString(),
            null);
    }

    public static RequestFailedException ConditionNotMet(ConditionType conditionType, string accountName, string blobContainerName, string message)
    {
        return new(
            412,
            $"Condition {conditionType} " +
            $"for container '{blobContainerName}' in account '{accountName} " +
            $"not met: {message}'.",
            BlobErrorCode.ConditionNotMet.ToString(),
            null);
    }
}
