using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.Faults;

namespace Datamole.InMemory.Azure.EventHubs.Faults;

public abstract record EventHubFault(EventHubFaultScope Scope) : Fault()
{
    public EventHubFault WithScope(Func<EventHubFaultScope, EventHubFaultScope> scopeFunc)
    {
        return this with { Scope = scopeFunc(Scope) };
    }

    public abstract EventHubsException CreateException(EventHubFaultScope currentScope);

    public record ServiceIsBusy(EventHubFaultScope Scope) : EventHubFault(Scope)
    {
        public override EventHubsException CreateException(EventHubFaultScope currentScope)
        {
            return EventHubClientExceptionFactory.ServiceIsBusy(currentScope);
        }
    }

}


