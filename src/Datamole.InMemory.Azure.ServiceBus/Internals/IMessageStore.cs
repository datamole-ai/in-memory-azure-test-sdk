using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal interface IMessageStore
{
    void AddMessage(ServiceBusMessage message);
    void AddMessages(IReadOnlyList<ServiceBusMessage> messages);
    Task<LockedSession?> TryAcquireNextAvailableSessionAsync(TimeSpan maxDelay, CancellationToken cancellationToken);
    LockedSession? TryAcquireSession(string sessionId);
    long ActiveMessageCount { get; }
    long MessageCount { get; }
}
