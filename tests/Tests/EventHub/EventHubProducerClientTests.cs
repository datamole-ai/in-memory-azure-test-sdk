using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;

using Datamole.InMemory.Azure.EventHubs;
using Datamole.InMemory.Azure.EventHubs.Clients;

namespace Tests.EventHub;

[TestClass]
public class EventHubProducerClientTests
{
    [TestMethod]
    public async Task SystemProperties_ShouldBeSent()
    {
        var eventHub = new InMemoryEventHubProvider().AddNamespace().AddEventHub("test-eh", 1);

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);
        await using var consumer = InMemoryPartitionReceiver.FromEventHub("$default", "0", eventHub, EventPosition.Earliest);

        var sentEventData = new EventData { MessageId = "test-mi", ContentType = "test-ct", CorrelationId = "test-ci" };

        producer.Send(sentEventData, new SendEventOptions { PartitionKey = "test-pk" });

        var batch = await consumer.ReceiveBatchAsync(100, TimeSpan.Zero);

        var eventData = batch.Should().ContainSingle().Which;

        eventData.MessageId.Should().Be("test-mi");
        eventData.ContentType.Should().Be("test-ct");
        eventData.CorrelationId.Should().Be("test-ci");
        eventData.PartitionKey.Should().Be("test-pk");

    }

}
