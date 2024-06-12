namespace Datamole.InMemory.Azure.EventHubs.Faults;

public record EventHubFaultBuilder(EventHubFaultScope Scope)
{
    public EventHubFault ServiceIsBusy() => new EventHubFault.ServiceIsBusy(Scope);
}
