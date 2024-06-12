using System.Collections.Concurrent;

using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal class SessionMessageStore(InMemoryServiceBusEntity entity) : IMessageStore
{
    private readonly TimeProvider _timeProvider = entity.TimeProvider;
    private readonly ConcurrentDictionary<string, SessionEngine> _sessions = new();

    public long ActiveMessageCount => _sessions.Values.Sum(s => s.ActiveMessageCount);
    public long MessageCount => _sessions.Values.Sum(s => s.MessageCount);

    public void AddMessage(ServiceBusMessage message)
    {
        if (!HasSessionId(message))
        {
            throw new InvalidOperationException("Message does not have a session id.");
        }

        var sessionId = message.SessionId;

        var session = _sessions.GetOrAdd(sessionId, (s) => new SessionEngine(sessionId, entity));

        session.AddMessage(message);
    }

    public void AddMessages(IReadOnlyList<ServiceBusMessage> messages)
    {
        if (messages.Any(m => !HasSessionId(m)))
        {
            throw new InvalidOperationException("All messages must have session id.");
        }

        foreach (var message in messages)
        {
            AddMessage(message);
        }
    }

    public async Task<LockedSession?> TryAcquireNextAvailableSessionAsync(TimeSpan maxDelay, CancellationToken cancellationToken)
    {
        var start = _timeProvider.GetTimestamp();

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var (_, sessionStore) in _sessions)
            {
                if (sessionStore.TryLockIfNotEmpty(out var acquiredSession))
                {
                    return acquiredSession;
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), timeProvider: _timeProvider, cancellationToken: cancellationToken);

        } while (_timeProvider.GetElapsedTime(start) < maxDelay);

        return null;
    }

    public LockedSession? TryAcquireSession(string sessionId)
    {
        if (_sessions.TryGetValue(sessionId, out var sessionStore))
        {
            if (sessionStore.TryLockIfNotEmpty(out var acquiredSession))
            {
                return acquiredSession;
            }
        }

        return null;
    }

    private static bool HasSessionId(ServiceBusMessage message) => !string.IsNullOrWhiteSpace(message.SessionId);
}
