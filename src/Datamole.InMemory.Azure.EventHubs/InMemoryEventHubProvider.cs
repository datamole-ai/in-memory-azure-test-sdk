using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

using Datamole.InMemory.Azure.EventHubs.Faults;
using Datamole.InMemory.Azure.EventHubs.Internals;
using Datamole.InMemory.Azure.Faults;
using Datamole.InMemory.Azure.Faults.Internals;

namespace Datamole.InMemory.Azure.EventHubs;

public class InMemoryEventHubProvider
{
    private readonly ConcurrentDictionary<string, InMemoryEventHubNamespace> _namespaces = new(StringComparer.OrdinalIgnoreCase);

    public string HostnameSuffix { get; }

    public InMemoryEventHubProvider(string? hostnameSuffix = null, TimeProvider? timeProvider = null)
    {
        HostnameSuffix = hostnameSuffix ?? "eventhub.in-memory.example.com";
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    internal FaultQueue Faults { get; } = new();

    internal TimeProvider TimeProvider { get; }

    public InMemoryEventHubNamespace GetNamespace(string namespaceName)
    {
        if (!TryGetNamespace(namespaceName, out var @namespace))
        {
            throw ExceptionFactory.NamespaceNotFound(namespaceName, this);
        }

        return @namespace;
    }

    public bool TryGetNamespace(string namespaceName, [NotNullWhen(true)] out InMemoryEventHubNamespace? @namespace)
    {
        if (!_namespaces.TryGetValue(namespaceName, out @namespace))
        {
            return false;
        }

        return true;
    }

    public InMemoryEventHubNamespace GetNamespaceByHostname(string namespaceHostname)
    {
        if (TryGetNamespaceByHostname(namespaceHostname, out var ns))
        {
            return ns;
        }

        throw ExceptionFactory.NamespaceNotFoundByHostname(namespaceHostname, this);
    }

    public bool TryGetNamespaceByHostname(string namespaceHostname, [NotNullWhen(true)] out InMemoryEventHubNamespace? @namespace)
    {
        foreach (var (name, ns) in _namespaces)
        {
            if (ns.Hostname == namespaceHostname)
            {
                @namespace = ns;
                return true;
            }
        }

        @namespace = null;
        return false;
    }

    public InMemoryEventHubNamespace AddNamespace(string? namespaceName = null)
    {
        namespaceName ??= GenerateNamespaceName();

        var @namespace = new InMemoryEventHubNamespace(namespaceName, this);

        if (!_namespaces.TryAdd(namespaceName, @namespace))
        {
            throw ExceptionFactory.NamespaceAlreadyExists(namespaceName, this);
        }

        return @namespace;
    }

    private static string GenerateNamespaceName() => Guid.NewGuid().ToString();

    public IFaultRegistration InjectFault(Func<EventHubFaultBuilder, Fault> faultAction, EventHubFaultScope? scope = null)
    {
        var fault = faultAction(new(scope ?? new()));

        return Faults.Inject(fault);
    }
}




