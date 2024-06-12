namespace Datamole.InMemory.Azure.EventHubs.Internals;

internal static class ExceptionFactory
{
    public static InvalidOperationException ConsumerGroupAlreadyExists(string consumerGroupName, InMemoryEventHub eventHub)
    {
        return new($"Consumer group '{consumerGroupName}' alredy exists in event hub '{eventHub.Name}' in namespace {eventHub.Namespace.Name}.");
    }

    public static InvalidOperationException PartitionNotFound(string partitionId, InMemoryEventHub eventHub)
    {
        return new($"Partition '{partitionId}' not found in event hub '{eventHub.Name}' in namespace {eventHub.Namespace.Name}.");
    }

    public static InvalidOperationException EventHubAlreadyExists(string eventHubName, InMemoryEventHubNamespace eventHubNamespace)
    {
        return new($"Event Hub '{eventHubName}' already exists in namespace '{eventHubNamespace.Name}'.");
    }

    public static InvalidOperationException EventHubNotFound(string eventHubName, InMemoryEventHubNamespace eventHubNamespace)
    {
        return new($"Event Hub '{eventHubName}' not found in namespace '{eventHubNamespace.Name}'.");
    }

    public static InvalidOperationException NamespaceAlreadyExists(string namespaceName, InMemoryEventHubProvider provider)
    {
        return new($"Event Hub namespace '{namespaceName}' already exists.");
    }

    public static InvalidOperationException NamespaceNotFound(string namespaceName, InMemoryEventHubProvider provider)
    {
        return new($"Event Hub namespace '{namespaceName}' not found.");
    }

    public static InvalidOperationException NamespaceNotFoundByHostname(string namespaceHostname, InMemoryEventHubProvider provider)
    {
        return new($"Event Hub namespace with hostname '{namespaceHostname}' not found.");
    }
}
