using System.Diagnostics.CodeAnalysis;

using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal class SessionEngine(string sessionId, InMemoryServiceBusEntity entity)
{
    private readonly object _syncObj = new();

    private readonly QueueEngine _queueEngine = new(entity);
    private readonly TimeProvider _timeProvider = entity.TimeProvider;

    private Guid? _sessionLockToken;
    private DateTimeOffset? _sessionLockedUntil;
    private BinaryData? _sessionState;

    public InMemoryServiceBusEntity ParentEntity { get; } = entity;
    public string FullyQualifiedNamespace { get; } = entity.FullyQualifiedNamespace;
    public string EntityPath { get; } = entity.EntityPath;
    public string SessionId { get; } = sessionId;
    public long ActiveMessageCount => _queueEngine.ActiveMessageCount;
    public long MessageCount => _queueEngine.MessageCount;

    public bool IsLocked
    {
        get { lock (_syncObj) { return _sessionLockToken.HasValue; } }
    }

    public DateTimeOffset? SessionLockedUntil
    {
        get { lock (_syncObj) { return _sessionLockedUntil; } }
    }

    public bool TryLockIfNotEmpty([NotNullWhen(true)] out LockedSession? acquiredSession)
    {
        lock (_syncObj)
        {
            if (_sessionLockToken is not null)
            {
                acquiredSession = null;
                return false;
            }

            if (_queueEngine.ActiveMessageCount > 0)
            {
                _sessionLockToken = Guid.NewGuid();
                _sessionLockedUntil = _timeProvider.GetUtcNow().Add(ParentEntity.LockTime);
                acquiredSession = new LockedSession(this, _sessionLockToken.Value);
                return true;
            }

            acquiredSession = null;
            return false;

        }
    }

    public void Release(LockedSession session)
    {
        lock (_syncObj)
        {
            if (_sessionLockToken == session.SessionLockToken)
            {
                _sessionLockToken = null;
                _sessionLockedUntil = null;
            }
        }
    }

    public async Task<SessionResult<IReadOnlyList<ServiceBusReceivedMessage>>> ReceiveAsync(LockedSession session, int maxMessages, TimeSpan maxWaitTime, ServiceBusReceiveMode receiveMode, CancellationToken cancellationToken)
    {
        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }
        }

        var messages = await _queueEngine.ReceiveAsync(maxMessages, maxWaitTime, receiveMode, cancellationToken);

        return new(messages);
    }

    public SessionResult RenewSessionLock(LockedSession session)
    {
        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }

            _sessionLockedUntil = _timeProvider.GetUtcNow().Add(ParentEntity.LockTime);
            return SessionResult.Successful;
        }
    }

    public SessionResult<BinaryData> GetSessionState(LockedSession session)
    {
        lock (_syncObj)
        {

            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }

            return new(_sessionState ?? new BinaryData(string.Empty));
        }
    }

    public SessionResult SetSessionState(LockedSession session, BinaryData state)
    {
        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }

            _sessionState = state;
        }

        return SessionResult.Successful;
    }

    public void AddMessage(ServiceBusMessage message)
    {
        AssertSession(message.SessionId);
        _queueEngine.AddMessage(message);
    }

    public SessionResult CompleteMessage(LockedSession session, ServiceBusReceivedMessage message)
    {
        AssertSession(message.SessionId);
        AssertSession(session.SessionId);

        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }
        }



        if (!_queueEngine.CompleteMessage(message))
        {
            return new(ServiceBusFailureReason.MessageLockLost);
        }

        return SessionResult.Successful;
    }

    public SessionResult AbandonMessage(LockedSession session, ServiceBusReceivedMessage message)
    {
        AssertSession(message.SessionId);

        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }
        }

        _queueEngine.AbandonMessage(message);

        return SessionResult.Successful;
    }

    public SessionResult RenewMessageLock(LockedSession session, ServiceBusReceivedMessage message)
    {
        AssertSession(message.SessionId);
        AssertSession(session.SessionId);

        lock (_syncObj)
        {
            if (!CheckSessionLockUnsafe(session))
            {
                return new(ServiceBusFailureReason.SessionLockLost);
            }
        }

        if (!_queueEngine.RenewMessageLock(message))
        {
            return new(ServiceBusFailureReason.MessageLockLost);
        }

        return SessionResult.Successful;
    }


    private bool CheckSessionLockUnsafe(LockedSession session)
    {

        if (_sessionLockToken != session.SessionLockToken)
        {
            return false;
        }

        if (_sessionLockedUntil is null || _sessionLockedUntil < _timeProvider.GetUtcNow())
        {
            return false;
        }

        return true;
    }

    private void AssertSession(string sessionIdFromIncomingMessage)
    {
        if (SessionId != sessionIdFromIncomingMessage)
        {
            throw new InvalidOperationException($"Message (sid = {sessionIdFromIncomingMessage}) does not belong to this session (sid = {SessionId}).");
        }
    }


}
