using Azure;

using Datamole.InMemory.Azure.Storage.Faults;
using Datamole.InMemory.Azure.Storage.Tables.Clients.Internals;

namespace Datamole.InMemory.Azure.Storage.Tables.Faults;

public abstract record TableStorageFault(TableStorageFaultScope Scope) : StorageFault<TableStorageFaultScope>(Scope)
{
    internal record ServiceIsBusy(TableStorageFaultScope Scope) : TableStorageFault(Scope)
    {
        public override RequestFailedException CreateException(TableStorageFaultScope currentScope)
        {
            return TableClientExceptionFactory.ServiceIsBusy(currentScope);
        }
    }
}
