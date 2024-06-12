using System.Security.Cryptography;
using System.Text;

using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs.Faults;

namespace Datamole.InMemory.Azure.EventHubs.Clients.Internals;

internal static class EventHubClientUtils
{
    public static InMemoryEventHub GetEventHub(InMemoryEventHubProvider provider, string namespaceHostname, string eventHubName)
    {
        if (!provider.TryGetNamespaceByHostname(namespaceHostname, out var ns))
        {
            throw EventHubClientExceptionFactory.NamespaceNotFound(namespaceHostname);
        }

        if (!ns.TryGetEventHub(eventHubName, out var eh))
        {
            throw EventHubClientExceptionFactory.EventHubNotFound(ns, eventHubName);
        }

        return eh;
    }

    public static void HasConsumerGroupOrThrow(InMemoryEventHub eventHub, string consumerGroupName)
    {
        if (!eventHub.HasConsumerGroup(consumerGroupName))
        {
            throw EventHubClientExceptionFactory.ConsumerGroupNotFound(eventHub, consumerGroupName);
        }
    }

    public static void CheckFaults(EventHubFaultScope currentScope, InMemoryEventHubProvider provider)
    {
        if (provider.Faults.TryGetFault<EventHubFault>(f => currentScope.IsSubscopeOf(f.Scope), out var fault))
        {
            throw fault.CreateException(currentScope);
        }
    }

    public static EventHubConnection GenerateConnection(string namespaceHostname, string eventHubName, string? keyName = null)
    {
        var connectionString = CreateConnectionStringForEventHub(namespaceHostname, eventHubName, keyName);
        return new(connectionString);
    }

    public static string CreateConnectionStringForEventHub(string namespaceHostname, string eventHubName, string? keyName = null)
    {
        var namespaceConnectionString = CreateConnectionStringForNamespace(namespaceHostname, keyName);
        return $"{namespaceConnectionString}EntityPath={eventHubName};";
    }

    public static string CreateConnectionStringForNamespace(string namespaceHostname, string? keyName = null)
    {
        keyName ??= "test-key";

        var keySeed = $"{keyName}|{namespaceHostname}";

        var key = Convert.ToBase64String(SHA384.HashData(Encoding.UTF8.GetBytes(keySeed)));

        return $"Endpoint=sb://{namespaceHostname};SharedAccessKey={key};SharedAccessKeyName={keyName};";
    }


}
