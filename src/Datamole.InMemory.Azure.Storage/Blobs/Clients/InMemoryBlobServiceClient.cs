using Azure.Storage.Blobs;

using Datamole.InMemory.Azure.Storage.Blobs.Internals;

namespace Datamole.InMemory.Azure.Storage.Blobs.Clients;

public class InMemoryBlobServiceClient : BlobServiceClient
{
    #region Constructors

    public InMemoryBlobServiceClient(string accountName, InMemoryStorageProvider provider)
        : this(InMemoryBlobService.CreateServiceUri(accountName, provider), provider)
    {
    }

    public InMemoryBlobServiceClient(Uri blobServiceUri, InMemoryStorageProvider provider)
    {
        Uri = blobServiceUri;
        Provider = provider;
    }

    public static InMemoryBlobServiceClient FromAccount(InMemoryStorageAccount account)
    {
        return new(account.BlobService.Uri, account.Provider);
    }

    #endregion

    public override Uri Uri { get; }
    public InMemoryStorageProvider Provider { get; }
    public override bool CanGenerateAccountSasUri => false;

    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
    {
        return new InMemoryBlobContainerClient(Uri, blobContainerName, Provider);
    }


}
