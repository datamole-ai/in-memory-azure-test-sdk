namespace Datamole.InMemory.Azure.ServiceBus.FluentAssertions;

public static class ShouldExtensions
{
    public static InMemoryServiceBusTopicSubscriptionAssertions Should(this InMemoryServiceBusTopicSubscription subscription) => new(subscription);

    public static InMemoryServiceBusQueueAssertions Should(this InMemoryServiceBusQueue topic) => new(topic);
}
