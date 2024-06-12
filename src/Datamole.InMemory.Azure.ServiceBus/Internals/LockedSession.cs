namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal record LockedSession(SessionEngine Store, Guid SessionLockToken)
{
    public string SessionId => Store.SessionId;

    public string FullyQualifiedNamesace => Store.FullyQualifiedNamespace;

    public string EntityPath => Store.EntityPath;

    public InMemoryServiceBusEntity ParentEntity => Store.ParentEntity;

}
