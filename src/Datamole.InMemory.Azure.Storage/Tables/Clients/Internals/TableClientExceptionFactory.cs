using Azure;
using Azure.Data.Tables.Models;

using Datamole.InMemory.Azure.Storage.Internals;
using Datamole.InMemory.Azure.Storage.Tables.Faults;
using Datamole.InMemory.Azure.Storage.Tables.Internals;

namespace Datamole.InMemory.Azure.Storage.Tables.Clients.Internals;

internal static class TableClientExceptionFactory
{
    public static HttpRequestException TableServiceNotFound(string accountName, InMemoryStorageProvider provider)
    {
        return new($"Host '{InMemoryTableService.CreateServiceUri(accountName, provider)}' not found.");
    }

    public static RequestFailedException TableNotFound(string tableName, InMemoryTableService tableService)
    {
        return new(404, $"Table '{tableName}' not found in '{tableService}'.", TableErrorCode.ResourceNotFound.ToString(), null);
    }

    public static RequestFailedException TableAlreadyExists(string accountName, string tableName)
    {
        return new(409, $"Table '{tableName}' already exists in account '{accountName}'.", TableErrorCode.TableAlreadyExists.ToString(), null);

    }

    public static RequestFailedException ServiceIsBusy(TableStorageFaultScope scope)
    {
        return new(503, $"Table service in account '{scope.StorageAccountName}' is busy.", "ServerBusy", null);
    }

    public static RequestFailedException EntityNotFound(string accountName, string tableName, string partitionKey, string rowKey)
    {
        return new(404, $"Entity '{partitionKey}/{rowKey}' not found in table '{tableName}' in account '{accountName}'.", TableErrorCode.EntityNotFound.ToString(), null);
    }

    public static RequestFailedException EntityAlreadyExists(string accountName, string tableName, string partitionKey, string rowKey)
    {
        return new(
            409,
            $"Entity '{partitionKey}/{rowKey}' already exist " +
            $"in table '{tableName}' in account '{accountName}'.",
            TableErrorCode.EntityAlreadyExists.ToString(),
            null);
    }

    public static RequestFailedException ConditionNotMet(string accountName, string tableName, string partitionKey, string rowKey, ConditionError error)
    {
        return new(
            412,
            $"Update condition '{error.ConditionType}' not satisfied " +
            $"for entity '{partitionKey}/{rowKey}' " +
            $"in table '{tableName}' in account '{accountName}': {error.Message}",
            TableErrorCode.UpdateConditionNotSatisfied.ToString(),
            null);
    }

    public static NotSupportedException FeatureNotSupported()
    {
        return new("This feature is not supported by the in-memory implementation.");
    }

    public static RequestFailedException MultiplePartitionsInTransaction(string accountName, string tableName)
    {
        return new(
            400,
            $"Entities with different partition keys in a single transaction are not allowed " +
            $"for table '{tableName}' in account '{accountName}'.",
            TableErrorCode.CommandsInBatchActOnDifferentPartitions.ToString(),
            null);
    }

    public static RequestFailedException TooManyEntitiesInTransaction(string accountName, string tableName, int maxCount, int actualCount)
    {
        return new(
            400,
            $"At most {maxCount} entities can be present in the transaction. Found {actualCount} entities in transaction " +
            $"for table '{tableName}' in account '{accountName}'.",
            TableErrorCode.InvalidInput.ToString(),
            null);
    }

    public static RequestFailedException DuplicateEntityInTransaction(string accountName, string tableName, string partitionKey, string rowKey)
    {
        return new(
            400,
            $"Entity '{partitionKey}/{rowKey}' is duplicated in the transaction " +
            $"for table '{tableName}' in account '{accountName}'.",
            TableErrorCode.InvalidDuplicateRow.ToString(),
            null);
    }
}
