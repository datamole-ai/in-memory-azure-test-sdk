namespace Datamole.InMemory.Azure.Storage.Tables.Faults;

public record TableStorageFaultBuilder(TableStorageFaultScope Scope)
{
    public TableStorageFault ServiceIsBusy() => new TableStorageFault.ServiceIsBusy(Scope);
}


