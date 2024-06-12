using FluentAssertions.Primitives;

using Datamole.InMemory.Azure.ServiceBus.FluentAssertions.Internal;

namespace Datamole.InMemory.Azure.ServiceBus.FluentAssertions;

public class InMemoryServiceBusTopicSubscriptionAssertions(InMemoryServiceBusTopicSubscription subject)
    : ReferenceTypeAssertions<InMemoryServiceBusTopicSubscription, InMemoryServiceBusTopicSubscriptionAssertions>(subject)
{
    protected override string Identifier => nameof(InMemoryServiceBusTopicSubscription);

    public async Task BeEmptyAsync(TimeSpan? maxWaitTime = null, string? because = null, params object[] becauseArgs)
    {
        var entity = $"{Subject.ParentTopic.TopicName}/{Subject.SubscriptionName}";

        await ServiceBusAssertionHelpers.EntityShouldBeEmptyAsync(entity, () => Subject.MessageCount, maxWaitTime, because, becauseArgs);
    }
}
