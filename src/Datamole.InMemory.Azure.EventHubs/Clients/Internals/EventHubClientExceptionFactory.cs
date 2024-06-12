using System.Net.Sockets;

using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs.Faults;

namespace Datamole.InMemory.Azure.EventHubs.Clients.Internals;

internal static class EventHubClientExceptionFactory
{
    public static NotSupportedException FeatureNotSupported(string? message = null)
    {
        const string baseMessage = "This SDK feature is not supported by in-memory implementation.";

        return message switch
        {
            string => new($"{baseMessage} {message}"),
            null => new(baseMessage),
        };
    }

    public static EventHubsException ConsumerGroupNotFound(InMemoryEventHub eventHub, string consumerGroupName)
    {
        return ResourceNotFound(consumerGroupName, $"Consumer Group '{consumerGroupName}' not found in '{eventHub}'.");
    }

    public static EventHubsException EventHubNotFound(InMemoryEventHubNamespace @namespace, string eventHubName)
    {
        return ResourceNotFound(eventHubName, $"Event Hub '{eventHubName}' not found in '{@namespace}'.");
    }

    public static SocketException NamespaceNotFound(string namespaceHostname)
    {
        return new SocketException(11001, $"No such host is known: {namespaceHostname}");
    }

    public static EventHubsException PartitionNotFound(InMemoryEventHub eh, string partitionId)
    {
        return ResourceNotFound(eh.Properties.Name, $"Partition '{partitionId}' not found in '{eh}'.");
    }
    public static EventHubsException ServiceIsBusy(EventHubFaultScope currentScope)
    {
        if (currentScope.PartitionId is not null)
        {
            return ResourceIsBusy(
                currentScope.EventHubName,
                $"Partition '{currentScope.PartitionId}' in event hub '{currentScope.EventHubName}' in namespace '{currentScope.NamespaceName}' is busy.");
        }
        else
        {
            return ResourceIsBusy(
                currentScope.EventHubName,
                $"Event hub '{currentScope.EventHubName}' in namespace '{currentScope.NamespaceName}' is busy.");
        }
    }

    private static EventHubsException ResourceIsBusy(string? eventHubName, string message)
    {
        return new(true, eventHubName, message, EventHubsException.FailureReason.ServiceBusy);
    }

    private static EventHubsException ResourceNotFound(string eventHubName, string message)
    {
        return new(false, eventHubName, message, EventHubsException.FailureReason.ResourceNotFound);
    }

}
