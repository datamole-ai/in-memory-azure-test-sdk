namespace Datamole.InMemory.Azure.Storage.Tables.Faults;

public enum TableOperation
{
    CreateTable,
    CreateTableIfNotExists,
    DeleteTable,
    GetEntity,
    QueryEntity,
    AddEntity,
    UpsertEntity,
    UpdateEntity,
    DeleteEntity,
    SubmitTransaction
}
