using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;

using Datamole.InMemory.Azure.EventHubs;
using Datamole.InMemory.Azure.EventHubs.Clients;

namespace Tests.EventHub;

[TestClass]
public class PartitionReceiverTests
{
    [TestMethod]
    public async Task SpecificStartingPosition_Inclusive_ShouldReturnOnlySpecificEvents()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1)
            .AddConsumerGroup("test-cg");

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);
        await using var receiver = InMemoryPartitionReceiver.FromEventHub("test-cg", "0", eventHub, EventPosition.FromSequenceNumber(1, isInclusive: true));

        producer.Send(new EventData(BinaryData.FromString("test-data-0")));
        producer.Send(new EventData(BinaryData.FromString("test-data-1")));
        producer.Send(new EventData(BinaryData.FromString("test-data-2")));
        producer.Send(new EventData(BinaryData.FromString("test-data-3")));

        var batch = await receiver.ReceiveBatchAsync(100);

        batch.Select(e => e.EventBody.ToString()).Should().Equal(new[] { "test-data-1", "test-data-2", "test-data-3" });

    }

    [TestMethod]
    public async Task FutureStartingPosition_ShouldReturnOnlyFutureEvents()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1)
            .AddConsumerGroup("test-cg");

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);
        await using var receiver = InMemoryPartitionReceiver.FromEventHub("test-cg", "0", eventHub, EventPosition.FromSequenceNumber(3, isInclusive: true));

        producer.Send(new EventData(BinaryData.FromString("test-data-0")));

        var batch1 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);
        batch1.Should().BeEmpty();

        producer.Send(new EventData(BinaryData.FromString("test-data-1")));

        var batch2 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);
        batch2.Should().BeEmpty();

        producer.Send(new EventData(BinaryData.FromString("test-data-2")));

        var batch3 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);
        batch3.Should().BeEmpty();

        producer.Send(new EventData(BinaryData.FromString("test-data-3")));

        var batch4 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);
        batch4.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-3");
    }

    [TestMethod]
    public async Task SpecificStartingPosition_Exclusive_ShouldReturnOnlySpecificEvents()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1)
            .AddConsumerGroup("test-cg");

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);

        producer.Send(new EventData(BinaryData.FromString("test-data-0")));
        producer.Send(new EventData(BinaryData.FromString("test-data-1")));
        producer.Send(new EventData(BinaryData.FromString("test-data-2")));
        producer.Send(new EventData(BinaryData.FromString("test-data-3")));

        await using var receiver = InMemoryPartitionReceiver.FromEventHub("test-cg", "0", eventHub, EventPosition.FromSequenceNumber(1, isInclusive: false));

        var batch = await receiver.ReceiveBatchAsync(100);

        batch.Select(e => e.EventBody.ToString()).Should().Equal(new[] { "test-data-2", "test-data-3" });

    }


    [TestMethod]
    public async Task LatestStartingPosition_ShouldReturnOnlyNewEvents()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1)
            .AddConsumerGroup("test-cg");

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);

        producer.Send(new EventData(BinaryData.FromString("test-data-1")));

        await using var receiver = InMemoryPartitionReceiver.FromEventHub("test-cg", "0", eventHub, EventPosition.Latest);

        var batch1 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch1.Should().BeEmpty();

        producer.Send(new EventData(BinaryData.FromString("test-data-2")));

        var batch2 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch2.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-2");

    }

    [TestMethod]
    public async Task EarliestStartingPosition_ShouldReturnAllEvents()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1)
            .AddConsumerGroup("test-cg");

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);

        producer.Send(new EventData(BinaryData.FromString("test-data-1")));

        await using var receiver = InMemoryPartitionReceiver.FromEventHub("test-cg", "0", eventHub, EventPosition.Earliest);

        var batch1 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch1.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-1");

        producer.Send(new EventData(BinaryData.FromString("test-data-2")));

        var batch2 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch2.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-2");

    }

    [TestMethod]
    public async Task By_Default_There_Should_Be_Default_Consumer_Group_And_Position_Is_Set_To_Earliest()
    {
        var eventHub = new InMemoryEventHubProvider()
            .AddNamespace()
            .AddEventHub("test-eh", 1);

        await using var producer = InMemoryEventHubProducerClient.FromEventHub(eventHub);

        producer.Send(new EventData(BinaryData.FromString("test-data-1")));

        await using var receiver = InMemoryPartitionReceiver.FromEventHub("0", eventHub);

        var batch1 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch1.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-1");

        producer.Send(new EventData(BinaryData.FromString("test-data-2")));

        var batch2 = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

        batch2.Should().ContainSingle(e => e.EventBody.ToString() == "test-data-2");

    }
}
