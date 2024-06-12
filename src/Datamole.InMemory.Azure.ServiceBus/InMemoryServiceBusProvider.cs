using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Datamole.InMemory.Azure.ServiceBus;
public class InMemoryServiceBusProvider(TimeProvider? timeProvider = null)
{
    private readonly ConcurrentDictionary<string, InMemoryServiceBusNamespace> _namespaces = new();

    public TimeProvider TimeProvider { get; } = timeProvider ?? TimeProvider.System;

    public InMemoryServiceBusNamespace AddNamespace(string? namespaceHostname = null)
    {
        namespaceHostname ??= $"{Guid.NewGuid()}.servicebus.in-memory.example.com";

        var ns = new InMemoryServiceBusNamespace(namespaceHostname, this);

        if (!_namespaces.TryAdd(namespaceHostname, ns))
        {
            throw new InvalidOperationException($"Service bus namespace {namespaceHostname} already added.");
        }

        return ns;
    }

    public bool TryGetNamespace(string fullyQualifiedNamespace, [NotNullWhen(true)] out InMemoryServiceBusNamespace? serviceBusNamespace)
    {
        return _namespaces.TryGetValue(fullyQualifiedNamespace, out serviceBusNamespace);
    }
}
