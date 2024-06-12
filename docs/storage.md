<h1 align="center">In-memory Test SDK for Azure Storage</h1>

<p align="center">This library provides in-memory SDK for Azure Storage which can be used as a drop-in replacement for the official 
<a href="https://www.nuget.org/packages/Azure.Storage.Blobs" target="_blank">Azure.Storage.Blobs</a> and 
<a href="https://www.nuget.org/packages/Azure.Data.Tables" target="_blank">Azure.Data.Tables</a> SDKs in your tests. </p>

<p align="center">
    <a href="#recommended-usage">Recommended Usage</a> |
    <a href="#fault-injection">Fault Injection</a> |
    <a href="#features">Features</a> |
    <a href="#available-blob-client-apis">Available Blob Client APIs</a> |
    <a href="#available-table-client-apis">Available Table Client APIs</a> |
    <a href="#available-fluent-assertions">Fluent Assertions</a>
</p>

> [!TIP]
> See the whole [In-Memory Azure Test SDK](../README.md) suite if you are interested in other Azure services.

## Recommended Usage

To get started, add `Datamole.InMemory.Azure.EventHubs` package to your project.

```shell
dotnet add Datamole.InMemory.Azure.Storage
```

Create custom interface for creating the SDK clients. This interface should be as specific for given use-case as possible to make it simple as possible. Also, provide default implementation that is creating real Azure SDK clients:
```cs
interface IStorageClientFactory
{
    BlobContainerClient CreateBlobContainerClient(Uri uri);
}

class StorageClientFactory(TokenCredential tokenCredential): IStorageClientFactory
{
    public BlobContainerClient CreateBlobContainerClient(Uri uri) => new(uri, tokenCredential);
}
```

Use this interface to obtain clients in tested code:

```cs
class ExampleService(IStorageClientFactory clientFactory)
{
    public async Task AddBlobToContainerAsync(BinaryData content, Uri containerUri, string blobName)
    {
        var containerClient = clientFactory.CreateBlobContainerClient(Uri uri);
        var blobClient = containerClient.GetBlobClient(blobName);
        await blobClient.UploadAsync(content);
    }
}
```

Provide implementation of `IStorageClientFactory` that is creating the in-memory clients:

```cs
class InMemoryStorageClientFactory(InMemoryStorageProvider provider): IStorageClientFactory
{
    public BlobContainerClient CreateBlobContainerClient(Uri uri)
    {
        return new InMemoryBlobContainerClient(uri, provider);
    }
}
```

When testing, it is now enough to initialize `InMemoryStorageProvider` to desired state and and inject `InMemoryStorageClientFactory` to the tested code (e.g. via Dependency Injection):

```cs
var storageAccount = new InMemoryStorageProvider().AddAccount();

var containerClient = InMemoryBlobContainerClient.FromAccount(storageAccount, "test-container");

containerClient.Create();

var services = new ServiceCollection();

services.AddSingleton<ExampleService>();
services.AddSingleton(storageAccount.Provider);
services.AddSingleton<IStorageClientFactory, InMemoryStorageClientFactory>();

var exampleService = services.BuildServiceProvider().GetRequiredService<ExampleService>();

var content = BinaryData.FromString("data");

await exampleService.AddBlobToContainerAsync(content, containerClient.Uri, "test-blob");

containerClient.GetBlobClient("test-blob").Exists().Value.Should.BeTrue();
```

## Fault Injection
Fault injections allows you to simulate transient and persistent faults in the Event Hub client.
Thanks to that you can test how your application behaves in case of Azure outages, network issues, timeouts, etc.

To inject a fault, you must call the `InjectFault` method on an `InMemoryStorageProvider` or `InMemoryStorageAccount` instance.
The first argument lets you define the type of the fault, and optionally its scope and the number of occurrences it should be active.

See the following example that simulates the "ServiceBusy" fault during the first query operation on the table:
```cs
var storageAccount = new InMemoryStorageProvider().AddAccount();

storageAccount.InjectFault(faults => faults
    .ForTableService()
    .ServiceIsBusy()
    .WithScope(scope => scope with {TableName = "TestTable", Operation = TableOperation.QueryEntity})
    .WithTransientOccurrences(1));

var tableClient = InMemoryTableClient.FromAccount(storageAccount, "TestTable");

tableClient.Create();

var act = () => tableClient.Query<TableEntity>(e => e.PartitionKey == "abc");

act.Should().Throw<RequestFailedException>();

act.Should().NotThrow();
```

### Persistent Faults
You can make the injected faults persistent simply by omitting the `WithTransientOccurrences` method call.
In this case, the fault will be active until you call the `Resolve` method on the `IFaultRegistration` instance which is returned by the `InjectFault` method.


```cs
var storageAccount = new InMemoryStorageProvider().AddAccount();

var fault = storageAccount.InjectFault(faults => faults
    .ForBlobService()
    .ServiceIsBusy()
    .WithScope(scope => scope with { Operation = BlobOperation.BlobCommitBlockList }));

var blobClient =  InMemoryBlockBlobClient.FromAccount(storageAccount, "test-container", "test-blob");

blobClient.GetParentBlobContainerClient().Create();

var stage = () => blobClient.StageBlock("id1", BinaryData.FromString("test").ToStream());
var commit = () => blobClient.CommitBlockList(new[] { "id1" });

stage.Should().NotThrow();

commit.Should().Throw<RequestFailedException>();

fault.Resolve();

commit.Should().NotThrow();
```

## Features

### Supported

Following features **are** supported:

Blobs:

* All public client properties.
* Client methods explicitly enumerated below.
* Blob metadata.
* Blob HTTP headers.
* Container metadata.
* `IfMatch`, `IfNoneMatch` conditions.

Tables:

* All public client properties.
* Client methods explicitly enumerated below.
* Conditional operations.
* Transactions.
* Both LINQ and string query filters.
* Both `Replace` and `Merge` update modes.

### Not Supported

Following features **are not** supported (behavior when using these features is undefined - it might throw an exception or ignore some parameters):

Blobs:

* Tags.
* Ranges (`HttpRange`).
* `IfModifiedSince`, `IfUnmodifiedSince` conditions.
* Leases.
* Access tiers.
* Legal holds, immutability policies
* Progress handling.
* Transfer (validation) options.

Tables:

* Query selectors.

## Available Blob Client APIs

### `InMemoryBlobServiceClient: BlobServiceClient`

**Containers**
* `GetBlobContainerClient(string blobContainerName)`

### `InMemoryBlobContainerClient: BlobContainerClient`

**Essentials**
* `GetParentBlobServiceClient()`
* `GetProperties(...)`
* `GetPropertiesAsync(...)`

**Existence**
* `Exists(...)`
* `ExistsAsync(...)`
* `Create(...)`
* `CreateAsync(...)`
* `CreateIfNotExists(...)`
* `CreateIfNotExistsAsync(...)`

**Blobs**
* `GetBlobClient(string blobName)`
* `GetBlockBlobClient(string blobName)`
* `GetBlobs(...)`
* `GetBlobsAsync(...)`

### `InMemoryBlobClient: BlobClient`

**Essentials**
* `GetParentBlobContainerClient()`
* `GetProperties(...)`
* `GetPropertiesAsync(...)`

**Existence**
* `Exists(...)`
* `ExistsAsync(...)`

**Downloading**
* `Download(...)`
* `DownloadAsync(...)`
* `DownloadStreaming(...)`
* `DownloadStreamingAsync(...)`
* `DownloadContent(...)`
* `DownloadContentAsync(...)`

**Uploading**
* `Upload(...)`
* `UploadAsync(...)`

### `InMemoryBlockBlobClient: BlockBlobClient`

**Essentials**
* `GetParentBlobContainerClient()`
* `GetProperties(...)`
* `GetPropertiesAsync(...)`

**Existence**
* `Exists(...)`
* `ExistsAsync(...)`

**Downloading**
* `Download(...)`
* `DownloadAsync(...)`
* `DownloadStreaming(...)`
* `DownloadStreamingAsync(...)`
* `DownloadContent(...)`
* `DownloadContentAsync(...)`

**Uploading**
* `Upload(...)`
* `UploadAsync(...)`

**Blocks**
* `GetBlockList(...)`
* `GetBlockListAsync(...)`
* `CommitBlockList(...)`
* `CommitBlockListAsync(...)`
* `StageBlock(...)`
* `StageBlockAsync(...)`


## Available Table Client APIs

### `InMemoryTableServiceClient : TableServiceClient`

**Tables**
* `GetTableClient(...)`

**Connection strings**
* `FromConnectionString(...)`

### `InMemoryTableClient : TableClient`

**Existence**
* `Create(...)`
* `CreateAsync(...)`
* `CreateIfNotExists(...)`
* `CreateIfNotExistsAsync(...)`

**Querying**
* `Query(...)`
* `QueryAsync(...)`

**Writing**
* `AddEntity(...)`
* `AddEntityAsync(...)`
* `UpsertEntity(...)`
* `UpsertEntityAsync(...)`
* `UpdateEntity(...)`
* `UpdateEntityAsync(...)`
* `DeleteEntity(...)`
* `DeleteEntityAsync(...)`

**Transactions**

* `SubmitTransaction(...)`
* `SubmitTransactionAsync(...)`

## Available Fluent Assertions

Namespace: `Datamole.InMemory.Azure.Storage.FluentAssertions`

### `BlobClientBase`

* `ExistAsync(...)`: returns immediately if the blob exists or waits for some time for the blob to be created before failing.
