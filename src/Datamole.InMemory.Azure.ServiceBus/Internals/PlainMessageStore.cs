using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal class PlainMessageStore(InMemoryServiceBusEntity entity) : IMessageStore
{
    private readonly QueueEngine _queueEngine = new(entity);

    public long ActiveMessageCount => _queueEngine.ActiveMessageCount;
    public long MessageCount => _queueEngine.MessageCount;

    public void AddMessage(ServiceBusMessage message)
    {
        if (HasSessionId(message))
        {
            throw new InvalidOperationException("Message must not have session id.");
        }

        _queueEngine.AddMessage(message);
    }

    public void AddMessages(IReadOnlyList<ServiceBusMessage> messages)
    {
        if (messages.Any(HasSessionId))
        {
            throw new InvalidOperationException("No messages must have session id.");
        }

        foreach (var message in messages)
        {
            AddMessage(message);
        }
    }

    public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, ServiceBusReceiveMode receiveMode, CancellationToken cancellationToken)
    {
        return _queueEngine.ReceiveAsync(maxMessages, maxWaitTime, receiveMode, cancellationToken);
    }

    public bool CompleteMessage(ServiceBusReceivedMessage message) => _queueEngine.CompleteMessage(message);

    public void AbandonMessage(ServiceBusReceivedMessage message) => _queueEngine.AbandonMessage(message);

    public bool RenewMessageLock(ServiceBusReceivedMessage message) => _queueEngine.RenewMessageLock(message);

    public Task<LockedSession?> TryAcquireNextAvailableSessionAsync(TimeSpan maxDelay, CancellationToken cancellationToken)
    {
        throw new InvalidOperationException($"Sessions are not enabled for entity {GetEntityId()}.");
    }

    public LockedSession? TryAcquireSession(string sessionId)
    {
        throw new InvalidOperationException($"Sessions are not enabled for entity {GetEntityId()}.");
    }

    private string GetEntityId() => $"'{entity.EntityPath}' in namespace '{entity.FullyQualifiedNamespace}'.";

    private bool HasSessionId(ServiceBusMessage message) => !string.IsNullOrWhiteSpace(message.SessionId);
}
