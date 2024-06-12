using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;
using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Internals;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients;

public class InMemoryBlobClient : BlobClient
{
    private readonly BlobClientCore _core;

    #region Constructors

    public InMemoryBlobClient(string accountName, string blobContainerName, string blobName, InMemoryStorageProvider provider)
        : this(InMemoryBlobService.CreateServiceUri(accountName, provider), blobContainerName, blobName, provider) { }

    public InMemoryBlobClient(Uri blobServiceUri, string blobContainerName, string blobName, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobServiceUri, blobContainerName, blobName), provider) { }

    public InMemoryBlobClient(Uri blobContainerUri, string blobName, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobContainerUri, blobName: blobName), provider) { }

    public InMemoryBlobClient(Uri blobUri, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobUri), provider) { }

    private InMemoryBlobClient(BlobUriBuilder uriBuilder, InMemoryStorageProvider provider)
    {
        _core = new(uriBuilder, provider);
    }

    public static InMemoryBlobClient FromAccount(InMemoryStorageAccount account, string blobContainerName, string blobName)
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

    #endregion

    #region Clients

    protected override BlobContainerClient GetParentBlobContainerClientCore() => _core.GetParentContainerClient();

    #endregion

    #region Upload

    private Response<BlobContentInfo> UploadCore(BinaryData content, BlobUploadOptions? options = null)
    {
        var info = _core.Upload(content, options);
        return InMemoryResponse.FromValue(info, 201);
    }


    private Response<BlobContentInfo> UploadCore(BinaryData content, bool overwrite)
    {
        BlobUploadOptions? options = null;

        if (!overwrite)
        {
            options ??= new();
            options.Conditions ??= new();
            options.Conditions.IfNoneMatch = ETag.All;
        }

        return UploadCore(content, options);
    }

    private Response<BlobContentInfo> UploadCore(Stream content, bool overwrite) => UploadCore(BinaryData.FromStream(content), overwrite);

    private Response<BlobContentInfo> UploadCore(Stream content, BlobUploadOptions? options = null) => UploadCore(BinaryData.FromStream(content), options);

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default) => UploadCore(content, options);

    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default) => UploadCore(content, options);

    public override Response<BlobContentInfo> Upload(Stream content) => UploadCore(content);

    public override Response<BlobContentInfo> Upload(BinaryData content) => UploadCore(content);

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content) => Task.FromResult(UploadCore(content));
    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content) => Task.FromResult(UploadCore(content));

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default) => Task.FromResult(UploadCore(content, options));

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default) => Task.FromResult(UploadCore(content, options));

    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken cancellationToken) => UploadCore(content);

    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken cancellationToken) => UploadCore(content);

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken) => Task.FromResult(UploadCore(content));

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken) => Task.FromResult(UploadCore(content));

    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite = false, CancellationToken cancellationToken = default) => UploadCore(content, overwrite);

    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite = false, CancellationToken cancellationToken = default) => UploadCore(content, overwrite);

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite = false, CancellationToken cancellationToken = default) => Task.FromResult(UploadCore(content, overwrite));

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite = false, CancellationToken cancellationToken = default) => Task.FromResult(UploadCore(content, overwrite));

    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders? httpHeaders = null, IDictionary<string, string>? metadata = null, BlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, AccessTier? accessTier = null, StorageTransferOptions transferOptions = default, CancellationToken cancellationToken = default)
    {
        var options = new BlobUploadOptions
        {
            HttpHeaders = httpHeaders,
            Metadata = metadata,
            Conditions = conditions,
            ProgressHandler = progressHandler,
            AccessTier = accessTier,
            TransferOptions = transferOptions
        };

        return UploadCore(content, options);
    }

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders? httpHeaders = null, IDictionary<string, string>? metadata = null, BlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, AccessTier? accessTier = null, StorageTransferOptions transferOptions = default, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(Upload(content, httpHeaders, metadata, conditions, progressHandler, accessTier, transferOptions, cancellationToken));
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

