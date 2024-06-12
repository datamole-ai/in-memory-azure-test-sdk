using Azure.Data.Tables;

using Datamole.InMemory.Azure.Storage.Tables.Internals;

namespace Datamole.InMemory.Azure.Storage.Tables.Clients;

public class InMemoryTableServiceClient : TableServiceClient
{

    #region Constructors

    public InMemoryTableServiceClient(string accountName, InMemoryStorageProvider provider)
        : this(InMemoryTableService.CreateServiceUri(accountName, provider), provider)
    {
    }

    public InMemoryTableServiceClient(Uri tableServiceUri, InMemoryStorageProvider provider)
    {
        Uri = tableServiceUri;
        Provider = provider;
        AccountName = new TableUriBuilder(tableServiceUri).AccountName;
    }

    public static InMemoryTableServiceClient FromConnectionString(string connectionString, InMemoryStorageProvider provider)
    {
        var serviceUri = InMemoryTableService.CreateServiceUriFromConnectionString(connectionString, provider);
        return new(serviceUri, provider);
    }

    public static InMemoryTableServiceClient FromAccount(InMemoryStorageAccount account)
    {
        return new(account.TableService.Uri, account.Provider);
    }

    #endregion

    public override Uri Uri { get; }
    public override string AccountName { get; }
    public InMemoryStorageProvider Provider { get; }

    public override TableClient GetTableClient(string tableName)
    {
        var uriBuilder = new TableUriBuilder(Uri)
        {
            Tablename = tableName
        };

        return new InMemoryTableClient(uriBuilder.ToUri(), Provider);
    }


}
