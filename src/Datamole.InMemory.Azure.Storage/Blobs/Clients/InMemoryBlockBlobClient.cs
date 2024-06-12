using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

using Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;
using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Internals;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients;

public class InMemoryBlockBlobClient : BlockBlobClient
{
    #region Constructors

    private readonly BlobClientCore _core;

    public InMemoryBlockBlobClient(string accountName, string blobContainerName, string blobName, InMemoryStorageProvider provider)
        : this(InMemoryBlobService.CreateServiceUri(accountName, provider), blobContainerName, blobName, provider) { }

    public InMemoryBlockBlobClient(Uri blobServiceUri, string blobContainerName, string blobName, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobServiceUri, blobContainerName, blobName), provider) { }

    public InMemoryBlockBlobClient(Uri blobContainerUri, string blobName, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobContainerUri, blobName: blobName), provider) { }

    public InMemoryBlockBlobClient(Uri blobUri, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobUri), provider) { }

    private InMemoryBlockBlobClient(BlobUriBuilder uriBuilder, InMemoryStorageProvider provider)
    {
        _core = new(uriBuilder, provider);
    }

    public static InMemoryBlockBlobClient FromAccount(InMemoryStorageAccount account, string blobContainerName, string blobName)
    {
        return new(account.BlobService.Uri, blobContainerName, blobName, account.Provider);
    }

    #endregion

    public InMemoryStorageProvider Provider => _core.Provider;

    #region Properties

    public override Uri Uri => _core.Uri;
    public override string AccountName => _core.AccountName;
    public override string BlobContainerName => _core.BlobContainerName;
    public override string Name => _core.Name;
    public override bool CanGenerateSasUri => false;

    public override int BlockBlobMaxUploadBlobBytes => InMemoryBlobService.MaxBlockSize;

    public override long BlockBlobMaxUploadBlobLongBytes => InMemoryBlobService.MaxBlockSize;

    public override int BlockBlobMaxStageBlockBytes => InMemoryBlobService.MaxBlockSize;

    public override long BlockBlobMaxStageBlockLongBytes => InMemoryBlobService.MaxBlockSize;

    public override int BlockBlobMaxBlocks => InMemoryBlobService.MaxBlockCount;

    #endregion

    #region Clients

    protected override BlobContainerClient GetParentBlobContainerClientCore() => _core.GetParentContainerClient();

    #endregion

    #region Get Block List

    public override Response<BlockList> GetBlockList(BlockListTypes blockListTypes = BlockListTypes.All, string? snapshot = null, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var blockList = _core.GetBlockList(blockListTypes, conditions);
        return InMemoryResponse.FromValue(blockList, 200);
    }

    public override Task<Response<BlockList>> GetBlockListAsync(BlockListTypes blockListTypes = BlockListTypes.All, string? snapshot = null, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var response = GetBlockList(blockListTypes, snapshot, conditions, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Stage Block

    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, BlockBlobStageBlockOptions? options = null, CancellationToken cancellationToken = default)
    {
        var blockInfo = _core.StageBlock(base64BlockId, BinaryData.FromStream(content), options);
        return InMemoryResponse.FromValue(blockInfo, 201);
    }

    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, byte[] transactionalContentHash, BlobRequestConditions conditions, IProgress<long> progressHandler, CancellationToken cancellationToken)
    {
        var options = new BlockBlobStageBlockOptions
        {
            Conditions = conditions
        };

        return StageBlock(base64BlockId, content, options, cancellationToken);
    }

    public override Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, byte[] transactionalContentHash, BlobRequestConditions conditions, IProgress<long> progressHandler, CancellationToken cancellationToken)
    {
        var response = StageBlock(base64BlockId, content, transactionalContentHash, conditions, progressHandler, cancellationToken);
        return Task.FromResult(response);
    }

    public override Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, BlockBlobStageBlockOptions? options = null, CancellationToken cancellationToken = default)
    {
        var response = StageBlock(base64BlockId, content, options, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Commit Block List

    public override Response<BlobContentInfo> CommitBlockList(
        IEnumerable<string> base64BlockIds,
        CommitBlockListOptions options,
        CancellationToken cancellationToken = default)
    {
        var contentInfo = _core.CommitBlockList(base64BlockIds, options);
        return InMemoryResponse.FromValue(contentInfo, 201);
    }

    public override Response<BlobContentInfo> CommitBlockList(
      IEnumerable<string> base64BlockIds,
      BlobHttpHeaders? httpHeaders = null,
      IDictionary<string, string>? metadata = null,
      BlobRequestConditions? conditions = null,
      AccessTier? accessTier = null,
      CancellationToken cancellationToken = default)
    {
        var options = new CommitBlockListOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions, AccessTier = accessTier };

        return CommitBlockList(base64BlockIds, options, cancellationToken);
    }

    public override Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, CommitBlockListOptions options, CancellationToken cancellationToken = default)
    {
        var response = CommitBlockList(base64BlockIds, options, cancellationToken);
        return Task.FromResult(response);
    }

    public override Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, BlobHttpHeaders? httpHeaders = null, IDictionary<string, string>? metadata = null, BlobRequestConditions? conditions = null, AccessTier? accessTier = null, CancellationToken cancellationToken = default)
    {
        var response = CommitBlockList(base64BlockIds, httpHeaders, metadata, conditions, accessTier, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Upload

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        var info = _core.Upload(BinaryData.FromStream(content), options);
        return InMemoryResponse.FromValue(info, 201);
    }

    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders? httpHeaders = null, IDictionary<string, string>? metadata = null, BlobRequestConditions? conditions = null, AccessTier? accessTier = null, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
    {
        var options = new BlobUploadOptions
        {
            HttpHeaders = httpHeaders,
            Metadata = metadata,
            Conditions = conditions,
            AccessTier = accessTier,
            ProgressHandler = progressHandler
        };
        return Upload(content, options, cancellationToken);
    }

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        var response = Upload(content, options, cancellationToken);
        return Task.FromResult(response);
    }

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders? httpHeaders = null, IDictionary<string, string>? metadata = null, BlobRequestConditions? conditions = null, AccessTier? accessTier = null, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
    {
        var response = Upload(content, httpHeaders, metadata, conditions, accessTier, progressHandler, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Exists

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        var exists = _core.Exists();

        return exists switch
        {
            true => InMemoryResponse.FromValue(true, 200),
            false => InMemoryResponse.FromValue(false, 404)
        };
    }

    public override Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default) => Task.FromResult(Exists(cancellationToken));

    #endregion

    #region Get properties
    public override Response<BlobProperties> GetProperties(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var properties = _core.GetProperties(conditions);
        return InMemoryResponse.FromValue(properties, 200);
    }

    public override Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default) => Task.FromResult(GetProperties(conditions, cancellationToken));

    #endregion

    #region Download

    public override Response<BlobDownloadInfo> Download(CancellationToken cancellationToken = default)
    {
        var info = _core.Download(null);
        return InMemoryResponse.FromValue(info, 200);
    }

    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var result = _core.DownloadStreaming(options);
        return InMemoryResponse.FromValue(result, 200);
    }

    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var content = _core.DownloadContent(options);
        return InMemoryResponse.FromValue(content, 200);
    }

    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken cancellationToken)
    {
        var options = new BlobDownloadOptions { Conditions = conditions };
        return DownloadContent(options, cancellationToken);
    }

    public override Response<BlobDownloadInfo> Download() => Download(default);
    public override Task<Response<BlobDownloadInfo>> DownloadAsync() => DownloadAsync(default);

    public override Task<Response<BlobDownloadInfo>> DownloadAsync(CancellationToken cancellationToken) => Task.FromResult(Download(cancellationToken));

    public override Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default) => Task.FromResult(DownloadStreaming(options, cancellationToken));

    public override Response<BlobDownloadResult> DownloadContent() => DownloadContent((BlobDownloadOptions?) null, default);

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync() => DownloadContentAsync(default);

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default) => DownloadContent((BlobDownloadOptions?) null, cancellationToken);

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken) => Task.FromResult(DownloadContent(cancellationToken));

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken) => DownloadContentAsync(conditions, cancellationToken);

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default) => Task.FromResult(DownloadContent(options, cancellationToken));

    #endregion



}

