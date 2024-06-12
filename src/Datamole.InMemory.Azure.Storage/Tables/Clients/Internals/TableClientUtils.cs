using Datamole.InMemory.Azure.Storage.Tables.Faults;

namespace Datamole.InMemory.Azure.Storage.Tables.Clients.Internals;

internal static class TableClientUtils
{
    public static void CheckFaults(TableStorageFaultScope currentScope, InMemoryStorageProvider provider)
    {
        if (provider.Faults.TryGetFault<TableStorageFault>(f => currentScope.IsSubscopeOf(f.Scope), out var fault))
        {
            throw fault.CreateException(currentScope);
        }
    }


}
