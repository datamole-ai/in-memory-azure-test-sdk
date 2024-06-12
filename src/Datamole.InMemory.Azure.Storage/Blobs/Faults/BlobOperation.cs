namespace Datamole.InMemory.Azure.Storage.Blobs.Faults;


public enum BlobOperation
{
    BlobStageBlock,
    BlobGetBlockList,
    BlobCommitBlockList,
    BlobDownload,
    BlobGetProperties,
    BlobExists,
    ContainerGetBlobs,
    ContainerExists,
    ContainerCreateIfNotExists,
    ContainerGetProperties,
    BlobUpload,
    ContainerCreate
}

