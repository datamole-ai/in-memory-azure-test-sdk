using Datamole.InMemory.Azure.Faults;

using Datamole.InMemory.Azure.Storage.Blobs.Internals;
using Datamole.InMemory.Azure.Storage.Faults;
using Datamole.InMemory.Azure.Storage.Internals;
using Datamole.InMemory.Azure.Storage.Tables.Internals;

namespace Datamole.InMemory.Azure.Storage;

public class InMemoryStorageAccount
{
    public InMemoryStorageAccount(string name, InMemoryStorageProvider provider)
    {
        Name = name;
        Provider = provider;

        TableService = new(this);
        BlobService = new(this);
    }

    public string Name { get; }
    public InMemoryStorageProvider Provider { get; }

    internal InMemoryTableService TableService { get; }
    internal InMemoryBlobService BlobService { get; }

    public string GetConnectionString() => ConnectionStringUtils.GetConnectionString(this);

    public Uri BlobServiceUri => BlobService.Uri;
    public Uri TableServiceUri => TableService.Uri;

    public override string ToString() => Name;

    public IFaultRegistration InjectFault(Func<StorageProviderFaultBuilder, Fault> faultAction)
    {
        return Provider.InjectFault(builder => faultAction(builder), new() { StorageAccountName = Name });
    }
}

