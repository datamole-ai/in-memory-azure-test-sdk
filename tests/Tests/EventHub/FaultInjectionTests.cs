using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;

using Datamole.InMemory.Azure.EventHubs;
using Datamole.InMemory.Azure.EventHubs.Clients;

namespace Tests.EventHub;

[TestClass]
public class FaultInjectionTests
{

    [TestMethod]
    public async Task Injected_Persistent_Fault_On_Namespace_Should_Be_Resolved()
    {
        var @namespace = new InMemoryEventHubProvider().AddNamespace();

        @namespace.AddEventHub("test-eh", 2).AddConsumerGroup("test-cg");

        var fault = @namespace.InjectFault(faults => faults.ServiceIsBusy());

        var receiver = InMemoryPartitionReceiver.FromNamespace("test-cg", "0", @namespace, "test-eh", EventPosition.Earliest);

        var act = () => receiver.ReceiveBatchAsync(10, TimeSpan.FromMilliseconds(10));

        await act
            .Should()
            .ThrowAsync<EventHubsException>()
            .Where(e => e.Reason == EventHubsException.FailureReason.ServiceBusy);

        fault.Resolve();

        await act.Should().NotThrowAsync();
    }

    [TestMethod]
    public async Task Injected_Persistent_Fault_On_EventHub_Should_Be_Resolved()
    {
        var eventHub = new InMemoryEventHubProvider().AddNamespace().AddEventHub("test-eh", 2);

        var fault = eventHub.InjectFault(faults => faults.ServiceIsBusy());

        var receiver = InMemoryPartitionReceiver.FromEventHub("0", eventHub);

        var act = () => receiver.ReceiveBatchAsync(10, TimeSpan.FromMilliseconds(10));

        await act
            .Should()
            .ThrowAsync<EventHubsException>()
            .Where(e => e.Reason == EventHubsException.FailureReason.ServiceBusy);

        fault.Resolve();

        await act.Should().NotThrowAsync();
    }


    [TestMethod]
    public async Task Injected_Transient_Fault_On_EventHub_Should_Be_Resolved()
    {
        var eventHub = new InMemoryEventHubProvider().AddNamespace().AddEventHub("test-eh", 2);

        eventHub.InjectFault(faults => faults.ServiceIsBusy().WithTransientOccurrences(1));

        var receiver = InMemoryPartitionReceiver.FromEventHub("0", eventHub);

        var act = () => receiver.ReceiveBatchAsync(10, TimeSpan.FromMilliseconds(10));

        await act
            .Should()
            .ThrowAsync<EventHubsException>()
            .Where(e => e.Reason == EventHubsException.FailureReason.ServiceBusy);

        await act.Should().NotThrowAsync();
    }
}
