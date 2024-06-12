using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs.Clients.Internals;
using Datamole.InMemory.Azure.EventHubs.Faults;
using Datamole.InMemory.Azure.EventHubs.Internals;
using Datamole.InMemory.Azure.Faults;

namespace Datamole.InMemory.Azure.EventHubs;

public class InMemoryEventHubNamespace
{
    private readonly ConcurrentDictionary<string, InMemoryEventHub> _eventHubs = new();

    public InMemoryEventHubNamespace(string name, InMemoryEventHubProvider provider)
    {
        Hostname = $"{name}.{provider.HostnameSuffix.TrimStart('.')}";
        Name = name;
        Provider = provider;
    }

    public string Hostname { get; }
    public string Name { get; }

    public InMemoryEventHubProvider Provider { get; }
    public EventHubFaultScope FaultScope => new() { NamespaceName = Name };

    public string GetConnectionString() => EventHubClientUtils.CreateConnectionStringForNamespace(Hostname);

    public InMemoryEventHub AddEventHub(string eventHubName, int numberOfPartitions, Action<InMemoryEventHubOptions>? optionsAction = null)
    {
        if (numberOfPartitions <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(numberOfPartitions), numberOfPartitions, "Number of partitions must be greater than 0.");
        }

        var partitionIds = Enumerable.Range(0, numberOfPartitions).Select(id => id.ToString()).ToArray();
        var properties = EventHubsModelFactory.EventHubProperties(eventHubName, DateTimeOffset.UtcNow, partitionIds);

        var options = new InMemoryEventHubOptions();

        optionsAction?.Invoke(options);

        var eventHub = new InMemoryEventHub(eventHubName, properties, options, this);

        if (!_eventHubs.TryAdd(eventHubName, eventHub))
        {
            throw ExceptionFactory.EventHubAlreadyExists(eventHubName, this);
        }

        return eventHub;

    }

    public InMemoryEventHub GetEventHub(string eventHubName)
    {
        if (!TryGetEventHub(eventHubName, out var eh))
        {
            throw ExceptionFactory.EventHubNotFound(eventHubName, this);
        }

        return eh;
    }

    public override string ToString() => $"{Name} ({Hostname})";

    public bool TryGetEventHub(string eventHubName, [NotNullWhen(true)] out InMemoryEventHub? eventHub)
    {
        return _eventHubs.TryGetValue(eventHubName, out eventHub);
    }

    public IFaultRegistration InjectFault(Func<EventHubFaultBuilder, Fault> faultAction, EventHubFaultScope? scope = null)
    {
        scope ??= new();
        scope = scope with { NamespaceName = Name };

        return Provider.InjectFault(builder => faultAction(builder), scope);
    }

    internal static string GetNamespaceNameFromHostname(string hostname, InMemoryEventHubProvider provider)
    {
        return hostname[..(hostname.Length - provider.HostnameSuffix.Length - 1)];
    }

}
