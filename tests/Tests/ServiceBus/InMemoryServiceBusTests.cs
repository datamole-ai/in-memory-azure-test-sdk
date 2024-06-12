using System.Net.Sockets;

using Azure.Messaging.ServiceBus;

using Microsoft.Extensions.Time.Testing;

using Datamole.InMemory.Azure.ServiceBus;
using Datamole.InMemory.Azure.ServiceBus.Clients;

namespace Tests.ServiceBus;

[TestClass]
public class InMemoryServiceBusTests
{
    [TestMethod]
    public async Task Queue_Receiver_Should_Receive_Message_Sent_After_Receive_Operation_Started()
    {
        var provider = new InMemoryServiceBusProvider();

        var queue = provider.AddNamespace().AddQueue("test-queue");

        await using var client = InMemoryServiceBusClient.FromNamespace(queue.Namespace);

        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        var receivedMessageTask = receiver.ReceiveMessageAsync();

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        var receivedMessage = await receivedMessageTask;

        receivedMessage.Body.ToString().Should().Be("Hello, world!");
    }

    [TestMethod]
    public async Task Topic_Receivers_Should_Receive_Message_Sent_After_Receive_Operation_Started()
    {
        var provider = new InMemoryServiceBusProvider();

        var topic = provider.AddNamespace().AddTopic("test-topic");

        topic.AddSubscription("subscription-1");
        topic.AddSubscription("subscription-2");

        await using var client = InMemoryServiceBusClient.FromNamespace(topic.Namespace);

        await using var sender = client.CreateSender("test-topic");
        await using var receiver1 = client.CreateReceiver("test-topic", "subscription-1");
        await using var receiver2 = client.CreateReceiver("test-topic", "subscription-2");

        var receivedMessageTask1 = receiver1.ReceiveMessageAsync();
        var receivedMessageTask2 = receiver2.ReceiveMessageAsync();

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        var receivedMessage1 = await receivedMessageTask1;
        var receivedMessage2 = await receivedMessageTask2;

        receivedMessage1.Body.ToString().Should().Be("Hello, world!");
        receivedMessage2.Body.ToString().Should().Be("Hello, world!");

    }

    [TestMethod]
    public async Task Abandoned_Message_Should_Be_Recieved_Again()
    {
        var provider = new InMemoryServiceBusProvider();

        var queue = provider.AddNamespace().AddQueue("test-queue");

        await using var client = InMemoryServiceBusClient.FromNamespace(queue.Namespace);

        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        var receivedMessage = await receiver.ReceiveMessageAsync();

        var additionalReceivedMessagesBeforeAbandon = await receiver.ReceiveMessagesAsync(1, TimeSpan.FromMilliseconds(100));

        additionalReceivedMessagesBeforeAbandon.Should().BeEmpty();

        await receiver.AbandonMessageAsync(receivedMessage);

        var additionalReceivedMessagesAfterAbandon = await receiver.ReceiveMessagesAsync(1, TimeSpan.FromMinutes(1));

        additionalReceivedMessagesAfterAbandon.Should().HaveCount(1);
        additionalReceivedMessagesAfterAbandon[0].Body.ToString().Should().Be("Hello, world!");
    }

    [TestMethod]
    public async Task Completed_Message_Should_Not_Be_Recieved_Again()
    {
        var timeProvider = new FakeTimeProvider();

        var provider = new InMemoryServiceBusProvider(timeProvider);

        var queue = provider.AddNamespace().AddQueue("test-queue");

        await using var client = InMemoryServiceBusClient.FromNamespace(queue.Namespace);

        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        var message = await receiver.ReceiveMessageAsync();

        await receiver.CompleteMessageAsync(message);

        timeProvider.Advance(TimeSpan.FromHours(1));

        var messagesAfterComplete = await receiver.ReceiveMessagesAsync(1, TimeSpan.FromMilliseconds(100));

        messagesAfterComplete.Should().BeEmpty();

    }

    [TestMethod]
    public async Task Expired_Message_Cannot_Be_Completed()
    {
        var timeProvider = new FakeTimeProvider();

        var provider = new InMemoryServiceBusProvider(timeProvider);

        var queue = provider.AddNamespace().AddQueue("test-queue", new() { LockTime = TimeSpan.FromMinutes(2) });

        await using var client = InMemoryServiceBusClient.FromNamespace(queue.Namespace);

        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        var message = await receiver.ReceiveMessageAsync();

        timeProvider.Advance(TimeSpan.FromMinutes(3));

        var act = () => receiver.CompleteMessageAsync(message);

        await act.Should()
            .ThrowAsync<ServiceBusException>()
            .Where(ex => ex.Reason == ServiceBusFailureReason.MessageLockLost);
    }

    [TestMethod]
    public async Task Missing_Namespace_Should_Throw()
    {
        var provider = new InMemoryServiceBusProvider();

        await using var client = new InMemoryServiceBusClient("test.example.com", provider);

        var act = () => client.CreateSender("test-queue").SendMessageAsync(new ServiceBusMessage());

        await act.Should()
            .ThrowAsync<SocketException>()
            .WithMessage("No such host is known: test.example.com");
    }

    [TestMethod]
    public async Task Missing_Entity_Should_Throw()
    {
        var provider = new InMemoryServiceBusProvider();

        var ns = provider.AddNamespace();

        await using var client = InMemoryServiceBusClient.FromNamespace(ns);
        await using var sender = client.CreateSender("test-queue");

        var act = () => sender.SendMessageAsync(new ServiceBusMessage());

        await act.Should()
            .ThrowAsync<ServiceBusException>()
            .Where(ex => ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound);
    }


    [TestMethod]
    public async Task Sending_Batch_Should_Succeed()
    {
        var provider = new InMemoryServiceBusProvider();
        var ns = provider.AddNamespace();
        var queue = ns.AddQueue("test-queue");

        await using var client = InMemoryServiceBusClient.FromNamespace(ns);

        await using var sender = client.CreateSender("test-queue");

        using var batch = await sender.CreateMessageBatchAsync();

        batch.TryAddMessage(new ServiceBusMessage(BinaryData.FromString("Message 1")));
        batch.TryAddMessage(new ServiceBusMessage(BinaryData.FromString("Message 2")));
        batch.TryAddMessage(new ServiceBusMessage(BinaryData.FromString("Message 3")));

        await sender.SendMessagesAsync(batch);

        await using var receiver = client.CreateReceiver("test-queue");

        var messages = await receiver.ReceiveMessagesAsync(3, TimeSpan.FromMilliseconds(100));

        messages.Select(messages => messages.Body.ToString()).Should().BeEquivalentTo(["Message 1", "Message 2", "Message 3"]);

    }


    [TestMethod]
    public async Task Message_Count_On_Queue_Should_Be_Reported()
    {
        var timeProvider = new FakeTimeProvider();

        var provider = new InMemoryServiceBusProvider(timeProvider);

        var queue = provider.AddNamespace().AddQueue("test-queue", new() { LockTime = TimeSpan.FromMinutes(2) });

        await using var client = InMemoryServiceBusClient.FromNamespace(queue.Namespace);

        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));
        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        queue.ActiveMessageCount.Should().Be(2);
        queue.MessageCount.Should().Be(2);

        var message = await receiver.ReceiveMessageAsync();

        queue.ActiveMessageCount.Should().Be(1);
        queue.MessageCount.Should().Be(2);

        await receiver.CompleteMessageAsync(message);

        queue.ActiveMessageCount.Should().Be(1);
        queue.MessageCount.Should().Be(1);

        _ = await receiver.ReceiveMessageAsync();

        queue.ActiveMessageCount.Should().Be(0);
        queue.MessageCount.Should().Be(1);

        timeProvider.Advance(TimeSpan.FromMinutes(3));

        queue.ActiveMessageCount.Should().Be(1);
        queue.MessageCount.Should().Be(1);
    }

    [TestMethod]
    public async Task Message_Count_On_Topic_Subscription_Should_Be_Reported()
    {
        var timeProvider = new FakeTimeProvider();

        var provider = new InMemoryServiceBusProvider(timeProvider);

        var topic = provider.AddNamespace().AddTopic("test-topic", new() { LockTime = TimeSpan.FromMinutes(2) });

        var subscription = topic.AddSubscription("test-subscription");

        await using var client = InMemoryServiceBusClient.FromNamespace(topic.Namespace);

        await using var sender = client.CreateSender("test-topic");
        await using var receiver = client.CreateReceiver("test-topic", "test-subscription");

        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));
        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("Hello, world!")));

        subscription.ActiveMessageCount.Should().Be(2);
        subscription.MessageCount.Should().Be(2);

        var message = await receiver.ReceiveMessageAsync();

        subscription.ActiveMessageCount.Should().Be(1);
        subscription.MessageCount.Should().Be(2);

        await receiver.CompleteMessageAsync(message);

        subscription.ActiveMessageCount.Should().Be(1);
        subscription.MessageCount.Should().Be(1);

        _ = await receiver.ReceiveMessageAsync();

        subscription.ActiveMessageCount.Should().Be(0);
        subscription.MessageCount.Should().Be(1);

        timeProvider.Advance(TimeSpan.FromMinutes(3));

        subscription.ActiveMessageCount.Should().Be(1);
        subscription.MessageCount.Should().Be(1);
    }

}
