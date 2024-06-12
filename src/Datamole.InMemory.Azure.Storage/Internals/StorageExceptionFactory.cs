namespace Datamole.InMemory.Azure.Storage.Internals;
internal static class StorageExceptionFactory
{
    public static InvalidOperationException StorageAccountNotFound(string accountName)
    {
        return new($"Storage account '{accountName}' not found.");
    }

    public static InvalidOperationException StorageAccountAlreadyExistsFound(string accountName)
    {
        return new($"Storage account '{accountName}' already exists.");
    }
}
