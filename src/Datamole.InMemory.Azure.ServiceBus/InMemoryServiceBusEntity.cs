using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus;

public abstract class InMemoryServiceBusEntity(string entityPath, InMemoryServiceBusEntityOptions options, InMemoryServiceBusNamespace serviceBusNamespace) : IEntityIdentity
{
    public string EntityPath { get; } = entityPath;
    public bool SessionEnabled { get; } = options.EnableSessions;
    public TimeSpan LockTime { get; } = options.LockTime ?? TimeSpan.FromSeconds(30);
    public InMemoryServiceBusNamespace Namespace { get; } = serviceBusNamespace;
    public TimeProvider TimeProvider => Namespace.TimeProvider;
    public InMemoryServiceBusProvider Provider => Namespace.Provider;

    public string FullyQualifiedNamespace => Namespace.FullyQualifiedNamespace;

    internal abstract void AddMessage(ServiceBusMessage message);
    internal abstract void AddMessages(IReadOnlyList<ServiceBusMessage> messages);
}
