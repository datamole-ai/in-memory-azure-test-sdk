using FluentAssertions.Primitives;

using Datamole.InMemory.Azure.ServiceBus.FluentAssertions.Internal;

namespace Datamole.InMemory.Azure.ServiceBus.FluentAssertions;

public class InMemoryServiceBusQueueAssertions(InMemoryServiceBusQueue subject)
    : ReferenceTypeAssertions<InMemoryServiceBusQueue, InMemoryServiceBusQueueAssertions>(subject)
{
    protected override string Identifier => nameof(InMemoryServiceBusQueue);

    public async Task BeEmptyAsync(TimeSpan? maxWaitTime = null, string? because = null, params object[] becauseArgs)
    {
        var entity = $"{Subject.QueueName}";

        await ServiceBusAssertionHelpers.EntityShouldBeEmptyAsync(entity, () => Subject.MessageCount, maxWaitTime, because, becauseArgs);
    }
}
