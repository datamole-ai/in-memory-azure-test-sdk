using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

using Datamole.InMemory.Azure.Storage.Blobs.Clients.Internals;
using Datamole.InMemory.Azure.Storage.Blobs.Faults;
using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Internals;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients;

public class InMemoryBlobContainerClient : BlobContainerClient
{

    #region Constructors

    public InMemoryBlobContainerClient(string accountName, string blobContainerName, InMemoryStorageProvider provider)
        : this(InMemoryBlobService.CreateServiceUri(accountName, provider), blobContainerName, provider) { }

    public InMemoryBlobContainerClient(Uri blobServiceUri, string blobContainerName, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobServiceUri, blobContainerName), provider) { }

    public InMemoryBlobContainerClient(Uri blobContainerUri, InMemoryStorageProvider provider)
        : this(BlobClientUtils.GetUriBuilder(blobContainerUri), provider) { }

    private InMemoryBlobContainerClient(BlobUriBuilder uriBuilder, InMemoryStorageProvider provider)
    {
        Uri = uriBuilder.ToUri();
        AccountName = uriBuilder.AccountName;
        Name = uriBuilder.BlobContainerName;
        Provider = provider;
    }

    public static InMemoryBlobContainerClient FromAccount(InMemoryStorageAccount account, string blobContainerName)
    {
        return new(account.BlobService.Uri, blobContainerName, account.Provider);
    }

    #endregion

    #region Properties

    public override Uri Uri { get; }
    public override string AccountName { get; }
    public override string Name { get; }
    public override bool CanGenerateSasUri => false;

    #endregion

    public InMemoryStorageProvider Provider { get; }

    private InMemoryBlobService GetBlobService()
    {
        if (!Provider.TryGetAccount(AccountName, out var account))
        {
            throw BlobClientExceptionFactory.BlobServiceNotFound(AccountName, Provider);
        }

        return account.BlobService;
    }

    private void CheckFaults(BlobOperation operation)
    {
        var scope = new BlobStorageFaultScope()
        {
            StorageAccountName = AccountName,
            BlobContainerName = Name,
            Operation = operation
        };

        BlobClientUtils.CheckFaults(scope, Provider);
    }

    private void CheckConditions(ETag? currentETag, BlobRequestConditions? conditions)
    {
        if (!ConditionChecker.CheckConditions(currentETag, conditions?.IfMatch, conditions?.IfNoneMatch, out var error))
        {
            throw BlobClientExceptionFactory.ConditionNotMet(error.ConditionType, AccountName, Name, error.Message);
        }
    }

    private InMemoryBlobContainer GetContainer()
    {
        var blobService = GetBlobService();

        if (!blobService.TryGetBlobContainer(Name, out var container))
        {
            throw BlobClientExceptionFactory.ContainerNotFound(Name, blobService);
        }

        return container;
    }



    private BlobContainerInfo GetInfo(InMemoryBlobContainer container)
    {
        var properties = container.GetProperties();
        return BlobsModelFactory.BlobContainerInfo(properties.ETag, properties.LastModified);
    }

    #region Get Client

    protected override BlobServiceClient GetParentBlobServiceClientCore()
    {
        var serviceUri = InMemoryBlobService.CreateServiceUri(AccountName, Provider);
        return new InMemoryBlobServiceClient(serviceUri, Provider);
    }

    public override BlobClient GetBlobClient(string blobName) => new InMemoryBlobClient(Uri, blobName, Provider);

    protected override BlobBaseClient GetBlobBaseClientCore(string blobName) => GetBlobClient(blobName);

    protected override BlockBlobClient GetBlockBlobClientCore(string blobName) => new InMemoryBlockBlobClient(Uri, blobName, Provider);

    #endregion

    #region Create If Not Exists

    private (BlobContainerInfo, bool) CreateIfNotExistsCore(IDictionary<string, string>? metadata)
    {
        CheckFaults(BlobOperation.ContainerCreateIfNotExists);

        var blobService = GetBlobService();

        var added = blobService.TryAddBlobContainer(Name, metadata, out var container);

        return (GetInfo(container), added);

    }

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string>? metadata = null, BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, CancellationToken cancellationToken = default)
    {
        (var info, var added) = CreateIfNotExistsCore(metadata);

        return added switch
        {
            true => InMemoryResponse.FromValue(info, 201),
            false => InMemoryResponse.FromValue(info, 409)
        };
    }

    public override Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string>? metadata = null, BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, CancellationToken cancellationToken = default)
    {
        var response = CreateIfNotExists(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        return Task.FromResult(response);
    }

    public override Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType, IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        return CreateIfNotExistsAsync(publicAccessType, metadata, null, cancellationToken);
    }

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType, IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        return CreateIfNotExists(publicAccessType, metadata, null, cancellationToken);
    }

    #endregion

    #region Create

    private BlobContainerInfo CreateCore(IDictionary<string, string>? metadata)
    {
        CheckFaults(BlobOperation.ContainerCreate);

        var blobService = GetBlobService();

        if (!blobService.TryAddBlobContainer(Name, metadata, out var container))
        {
            throw BlobClientExceptionFactory.ContainerAlreadyExists(AccountName, Name);
        }

        return GetInfo(container);
    }

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string>? metadata = null, BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, CancellationToken cancellationToken = default)
    {
        var info = CreateCore(metadata);
        return InMemoryResponse.FromValue(info, 201);
    }

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        return Create(publicAccessType, metadata, null, cancellationToken);
    }

    public override Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string>? metadata = null, BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, CancellationToken cancellationToken = default)
    {
        var response = Create(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        return Task.FromResult(response);
    }

    public override Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType, IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        return CreateAsync(publicAccessType, metadata, null, cancellationToken);
    }

    #endregion

    #region Get Blobs
    private IEnumerable<Page<BlobItem>> GetBlobsCore(string? prefix)
    {
        CheckFaults(BlobOperation.ContainerGetBlobs);

        var container = GetContainer();

        var blobs = container.GetBlobs();

        var pages = blobs
            .Select(b => b.AsBlobItem())
            .Where(b => prefix is null ? true : b.Name.StartsWith(prefix))
            .Chunk(2000)
            .Select((e, i) => Page<BlobItem>.FromValues(e, $"ct{i}", new InMemoryResponse(200)));


        return pages;
    }

    public override AsyncPageable<BlobItem> GetBlobsAsync(
        BlobTraits traits = BlobTraits.None,
        BlobStates states = BlobStates.None,
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        var pages = GetBlobsCore(prefix);

        return AsyncPageable<BlobItem>.FromPages(pages);
    }



    public override Pageable<BlobItem> GetBlobs(
        BlobTraits traits = BlobTraits.None,
        BlobStates states = BlobStates.None,
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        var pages = GetBlobsCore(prefix);

        return Pageable<BlobItem>.FromPages(pages);
    }

    #endregion

    #region Exists

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        CheckFaults(BlobOperation.ContainerExists);

        var service = GetBlobService();

        return service.ContainerExists(Name) switch
        {
            true => InMemoryResponse.FromValue(true, 200),
            false => InMemoryResponse.FromValue(false, 404)
        };
    }

    public override Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    {
        var result = Exists(cancellationToken);
        return Task.FromResult(result);
    }

    #endregion

    #region Get Properties

    public override Response<BlobContainerProperties> GetProperties(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        CheckFaults(BlobOperation.ContainerGetProperties);

        var container = GetContainer();

        CheckConditions(container.GetProperties().ETag, conditions);

        return InMemoryResponse.FromValue(container.GetProperties(), 200);
    }

    public override Task<Response<BlobContainerProperties>> GetPropertiesAsync(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(GetProperties(conditions, cancellationToken));
    }

    #endregion

}
