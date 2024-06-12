using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;

using Datamole.InMemory.Azure.Storage.Internals;
using Datamole.InMemory.Azure.Storage.Tables.Clients.Internals;
using Datamole.InMemory.Azure.Storage.Tables.Faults;
using Datamole.InMemory.Azure.Storage.Tables.Internals;

namespace Datamole.InMemory.Azure.Storage.Tables.Clients;

public class InMemoryTableClient : TableClient
{

    #region Constructors

    public InMemoryTableClient(string accountName, string tableName, InMemoryStorageProvider provider)
        : this(InMemoryTableService.CreateServiceUri(accountName, provider), tableName, provider) { }

    public InMemoryTableClient(Uri tableServiceUri, string tableName, InMemoryStorageProvider provider)
        : this(GetUriBuilder(tableServiceUri, tableName), provider) { }

    public InMemoryTableClient(Uri tableUri, InMemoryStorageProvider provider)
        : this(GetUriBuilder(tableUri), provider) { }

    private InMemoryTableClient(TableUriBuilder uriBuilder, InMemoryStorageProvider provider)
    {
        Uri = uriBuilder.ToUri();
        AccountName = uriBuilder.AccountName;
        Name = uriBuilder.Tablename;
        Provider = provider;
    }

    public static InMemoryTableClient FromAccount(InMemoryStorageAccount account, string tableName)
    {
        return new(account.BlobService.Uri, tableName, account.Provider);
    }

    private static TableUriBuilder GetUriBuilder(Uri uri, string? tableName = null)
    {
        var builder = new TableUriBuilder(uri);

        if (tableName is not null)
        {
            builder.Tablename = tableName;
        }

        return builder;
    }

    #endregion

    public override Uri Uri { get; }
    public override string Name { get; }
    public override string AccountName { get; }

    public InMemoryStorageProvider Provider { get; }

    private void CheckFaults(TableOperation operation)
    {
        var scope = new TableStorageFaultScope()
        {
            StorageAccountName = AccountName,
            TableName = Name,
            Operation = operation
        };

        TableClientUtils.CheckFaults(scope, Provider);
    }

    private InMemoryTableService GetService()
    {
        if (!Provider.TryGetAccount(AccountName, out var account))
        {
            throw TableClientExceptionFactory.TableServiceNotFound(AccountName, Provider);
        }

        return account.TableService;
    }

    private InMemoryTable GetTable()
    {
        if (!TryGetTable(out var service, out var table))
        {
            throw TableClientExceptionFactory.TableNotFound(Name, service);
        }

        return table;
    }

    private bool TryGetTable(out InMemoryTableService service, [NotNullWhen(true)] out InMemoryTable? table)
    {
        service = GetService();

        if (!service.TryGetTable(Name, out table))
        {
            return false;
        }

        return true;
    }

    #region Create Table If Not Exists

    private (TableItem, bool) CreateIfNotExistsCore()
    {
        CheckFaults(TableOperation.CreateTableIfNotExists);

        var service = GetService();

        var added = service.TryAddTable(Name, out var table);

        return (table.AsItem(), added);

    }

    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
    {
        (var item, var added) = CreateIfNotExistsCore();

        return added switch
        {
            true => InMemoryResponse.FromValue(item, 201),
            false => InMemoryResponse.FromValue(item, 409)
        };

    }

    public override Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
    {
        var result = CreateIfNotExists(cancellationToken);
        return Task.FromResult(result);
    }

    #endregion

    #region Create

    private TableItem CreateCore()
    {
        CheckFaults(TableOperation.CreateTable);

        var service = GetService();

        if (!service.TryAddTable(Name, out var table))
        {
            throw TableClientExceptionFactory.TableAlreadyExists(AccountName, Name);
        }

        return table.AsItem();
    }

    public override Response<TableItem> Create(CancellationToken cancellationToken = default)
    {
        var item = CreateCore();
        return InMemoryResponse.FromValue(item, 201);
    }

    public override Task<Response<TableItem>> CreateAsync(CancellationToken cancellationToken = default)
    {
        var response = Create(cancellationToken);
        return Task.FromResult(response);
    }


    #endregion

    #region Query

    private IEnumerable<Page<T>> QueryCore<T>(Func<T, bool> typedFilter, int? maxPerPage) where T : class, ITableEntity
    {
        return QueryCore(entities => entities.Select(e => e.ToAzureTableEntity<T>()).Where(typedFilter), maxPerPage);
    }

    private IEnumerable<Page<T>> QueryCore<T>(Func<InMemoryTableEntity, bool> genericFilter, int? maxPerPage) where T : class, ITableEntity
    {
        return QueryCore(e => e.Where(genericFilter).Select(e => e.ToAzureTableEntity<T>()), maxPerPage);
    }

    private IEnumerable<Page<T>> QueryCore<T>(Func<IEnumerable<InMemoryTableEntity>, IEnumerable<T>> filter, int? maxPerPage) where T : class, ITableEntity
    {
        CheckFaults(TableOperation.QueryEntity);

        var table = GetTable();

        var filteredEntities = filter(table.GetEntities());

        var pages = filteredEntities
            .Chunk(maxPerPage ?? 2000)
            .Select((e, i) => Page<T>.FromValues(e, $"ct{i}", new InMemoryResponse(200)));

        return pages;
    }

    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var filterCompiled = filter.Compile();

        var pages = QueryCore(filterCompiled, maxPerPage);

        return Pageable<T>.FromPages(pages);
    }

    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var filterCompiled = filter.Compile();

        var pages = QueryCore(filterCompiled, maxPerPage);

        return AsyncPageable<T>.FromPages(pages);
    }

    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var matcher = new TextQueryFilterMatcher(filter, Provider.LoggerFactory);

        var pages = QueryCore<T>(matcher.IsMatch, maxPerPage);

        return Pageable<T>.FromPages(pages);
    }


    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var matcher = new TextQueryFilterMatcher(filter, Provider.LoggerFactory);

        var pages = QueryCore<T>(matcher.IsMatch, maxPerPage);

        return AsyncPageable<T>.FromPages(pages);
    }

    #endregion

    #region Upsert & Update Entity

    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.UpsertEntity);

        return UpsertCore(entity, entity.ETag, mode);
    }

    public override Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var response = UpsertEntity(entity, mode, cancellationToken);
        return Task.FromResult(response);
    }



    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.UpdateEntity);

        return UpsertCore(entity, ifMatch, mode, mustExist: true);
    }

    public override Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var response = UpdateEntity(entity, ifMatch, mode, cancellationToken);
        return Task.FromResult(response);
    }

    private InMemoryResponse UpsertCore<T>(T entity, ETag ifMatch, TableUpdateMode mode, bool mustExist = false) where T : ITableEntity
    {
        if (ifMatch.IsEmpty())
        {
            ifMatch = ETag.All;
        }

        var table = GetTable();

        if (!table.TryUpsertEntity(entity, ifMatch, mode, mustExist, out var eTag, out var error))
        {
            throw error.GetClientException();
        }

        return new(204, eTag: eTag.Value);
    }

    #endregion

    #region Delete Entity

    public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.DeleteEntity);

        var table = GetTable();

        if (ifMatch.IsEmpty())
        {
            ifMatch = ETag.All;
        }

        if (!table.TryDeleteEntity(partitionKey, rowKey, ifMatch, out var error))
        {
            throw error.GetClientException();
        }

        return new InMemoryResponse(204);

    }

    public override Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var result = DeleteEntity(partitionKey, rowKey, ifMatch, cancellationToken);
        return Task.FromResult(result);
    }

    #endregion

    #region Add Entity

    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.AddEntity);

        var table = GetTable();

        if (!table.TryAddEntity(entity, out var eTag, out var error))
        {
            throw error.GetClientException();
        }

        return new InMemoryResponse(204, eTag: eTag.Value);
    }

    public override Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
    {
        var response = AddEntity(entity, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Table Exists

    public bool Exists() => TryGetTable(out _, out _);

    #endregion

    #region Get Entity

    private bool TryGetEntityCore<T>(string partitionKey, string rowKey, [NotNullWhen(true)] out T? result) where T : ITableEntity
    {
        CheckFaults(TableOperation.GetEntity);

        var table = GetTable();

        foreach (var inMemoryEntity in table.GetEntities())
        {
            if (inMemoryEntity.PartitionKey.Equals(partitionKey, StringComparison.Ordinal) && inMemoryEntity.RowKey.Equals(rowKey, StringComparison.Ordinal))
            {
                result = inMemoryEntity.ToAzureTableEntity<T>();
                return true;
            }
        }

        result = default;
        return false;
    }

    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        if (!TryGetEntityCore<T>(partitionKey, rowKey, out var entity))
        {
            throw TableClientExceptionFactory.EntityNotFound(AccountName, Name, partitionKey, rowKey);
        }

        return InMemoryResponse.FromValue(entity, 200, eTag: entity.ETag);
    }

    public override Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var response = GetEntity<T>(partitionKey, rowKey, select, cancellationToken);
        return Task.FromResult(response);
    }

    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        if (TryGetEntityCore<T>(partitionKey, rowKey, out var entity))
        {
            return InMemoryNullableResponse<T>.FromValue(entity);
        }
        else
        {
            return InMemoryNullableResponse<T>.FromNull();
        }
    }

    public override Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var response = GetEntityIfExists<T>(partitionKey, rowKey, select, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region Transaction

    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.SubmitTransaction);

        var table = GetTable();

        var transactions = transactionActions.ToList();

        if (!table.TrySubmitTransaction(transactions, out var results, out var error))
        {
            throw error.GetClientException();
        }

        if (results.Count != transactions.Count)
        {
            throw new InvalidOperationException("Transaction results count does not match transaction actions count.");
        }

        var responses = new List<InMemoryResponse>();

        for (var i = 0; i < results.Count; i++)
        {
            var t = transactions[i];
            var r = results[i];

            var status = t.ActionType switch
            {
                TableTransactionActionType.Add => 204,
                TableTransactionActionType.UpdateMerge => 204,
                TableTransactionActionType.UpdateReplace => 204,
                TableTransactionActionType.Delete => 204,
                TableTransactionActionType.UpsertMerge => 204,
                TableTransactionActionType.UpsertReplace => 204,
            };

            responses.Add(new(status, r.ETag));
        }

        return InMemoryResponse.FromValue<IReadOnlyList<Response>>(responses, 202);

    }

    public override Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    {
        var response = SubmitTransaction(transactionActions, cancellationToken);
        return Task.FromResult(response);
    }

    #endregion

    #region SAS

    public override TableSasBuilder GetSasBuilder(TableSasPermissions permissions, DateTimeOffset expiresOn) => new(Name, permissions, expiresOn);

    public override TableSasBuilder GetSasBuilder(string rawPermissions, DateTimeOffset expiresOn) => new(Name, rawPermissions, expiresOn);

    public override Uri GenerateSasUri(TableSasPermissions permissions, DateTimeOffset expiresOn) => Uri;

    public override Uri GenerateSasUri(TableSasBuilder builder)
    {
        if (builder.TableName != Name)
        {
            throw new InvalidOperationException($"Table name in the builder ({builder.TableName}) does not match actual table name ({Name}).");
        }

        return Uri;
    }

    #endregion

    #region Delete Table

    public override Response Delete(CancellationToken cancellationToken = default)
    {
        CheckFaults(TableOperation.DeleteTable);

        var service = GetService();

        if (!service.TryDeleteTable(Name))
        {
            throw TableClientExceptionFactory.TableNotFound(Name, service);
        }

        return new InMemoryResponse(204);
    }

    public override Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    {
        var result = Delete(cancellationToken);
        return Task.FromResult(result);
    }

    #endregion

    #region Unsupported

    public override Task<Response<IReadOnlyList<TableSignedIdentifier>>> GetAccessPoliciesAsync(CancellationToken cancellationToken = default)
    {
        throw TableClientExceptionFactory.FeatureNotSupported();
    }

    public override Response<IReadOnlyList<TableSignedIdentifier>> GetAccessPolicies(CancellationToken cancellationToken = default)
    {
        throw TableClientExceptionFactory.FeatureNotSupported();
    }

    public override Task<Response> SetAccessPolicyAsync(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken cancellationToken = default)
    {
        throw TableClientExceptionFactory.FeatureNotSupported();
    }

    public override Response SetAccessPolicy(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken cancellationToken = default)
    {
        throw TableClientExceptionFactory.FeatureNotSupported();
    }

    #endregion

}
