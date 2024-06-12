using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus;

public class InMemoryServiceBusQueue : InMemoryServiceBusEntity
{
    public InMemoryServiceBusQueue(string queueName, InMemoryServiceBusEntityOptions options, InMemoryServiceBusNamespace serviceBusNamespace) : base(queueName, options, serviceBusNamespace)
    {
        QueueName = queueName;
        MessageStore = MessageStoreFactory.CreateMessageStore(this);
    }

    public string QueueName { get; }

    public long ActiveMessageCount => MessageStore.ActiveMessageCount;
    public long MessageCount => MessageStore.MessageCount;

    internal IMessageStore MessageStore { get; }

    internal override void AddMessage(ServiceBusMessage message) => MessageStore.AddMessage(message);

    internal override void AddMessages(IReadOnlyList<ServiceBusMessage> messages) => MessageStore.AddMessages(messages);

}
