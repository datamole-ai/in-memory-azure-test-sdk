using Azure.Messaging.ServiceBus;

using Datamole.InMemory.Azure.ServiceBus.Clients.Internals;
using Datamole.InMemory.Azure.ServiceBus.Internals;

namespace Datamole.InMemory.Azure.ServiceBus.Clients;

public class InMemoryServiceBusSessionReceiver : ServiceBusSessionReceiver
{
    private readonly LockedSession _session;
    private readonly InMemoryServiceBusEntity _parentEntity;
    private readonly TimeSpan _defaultMaxWaitTime;
    private readonly string _identifier;
    private readonly int _prefetchCount;
    private readonly SessionEngine _store;

    private bool _isClosed;

    internal InMemoryServiceBusSessionReceiver(LockedSession session, ServiceBusSessionReceiverOptions options, TimeSpan defaultMaxWaitTime)
    {
        _session = session;
        _store = session.Store;
        _parentEntity = session.ParentEntity;
        _defaultMaxWaitTime = defaultMaxWaitTime;
        ReceiveMode = options.ReceiveMode;
        _identifier = options.Identifier ?? Guid.NewGuid().ToString();
        _prefetchCount = options.PrefetchCount;
    }

    public override string FullyQualifiedNamespace => _parentEntity.Namespace.FullyQualifiedNamespace;
    public override string EntityPath => _parentEntity.EntityPath;
    public override ServiceBusReceiveMode ReceiveMode { get; }
    public override string Identifier => _identifier;
    public override int PrefetchCount => _prefetchCount;
    public override string SessionId => _session.SessionId;
    public override bool IsClosed => _isClosed;
    public override DateTimeOffset SessionLockedUntil => _store.SessionLockedUntil ?? DateTimeOffset.MinValue;

    public override async Task CloseAsync(CancellationToken cancellationToken = default) => await DisposeAsync();

    public override ValueTask DisposeAsync()
    {
        _store.Release(_session);
        _isClosed = true;
        return ValueTask.CompletedTask;
    }

    #region Receive

    public override async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
    {
        var result = await _store.ReceiveAsync(_session, maxMessages, maxWaitTime ?? _defaultMaxWaitTime, ReceiveMode, cancellationToken);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionReceiveFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return result.Value;
    }

    public override IAsyncEnumerable<ServiceBusReceivedMessage> ReceiveMessagesAsync(CancellationToken cancellationToken = default)
    {
        return ServiceBusClientUtils.ReceiveAsAsyncEnumerable(_session, ReceiveMode, cancellationToken);
    }

    public override Task<ServiceBusReceivedMessage?> ReceiveMessageAsync(TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
    {
        return ServiceBusClientUtils.ReceiveSingleAsync(_session, maxWaitTime ?? _defaultMaxWaitTime, ReceiveMode, cancellationToken);
    }

    #endregion

    #region Abandon, Complete, Renew Message

    public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
    {
        if (propertiesToModify is not null)
        {
            throw ServiceBusClientExceptionFactory.FeatureNotSupported("Properties cannot be modified.");
        }

        var result = _store.AbandonMessage(_session, message);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionAbandonMessageFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }


        return Task.CompletedTask;
    }

    public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
    {
        var result = _store.CompleteMessage(_session, message);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionCompleteMessageFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return Task.CompletedTask;
    }

    public override Task RenewMessageLockAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
    {
        var result = _store.RenewMessageLock(_session, message);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionRenewMessageFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region Renew Session

    public override Task RenewSessionLockAsync(CancellationToken cancellationToken = default)
    {
        var result = _store.RenewSessionLock(_session);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionRenewFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region Session State

    public override Task<BinaryData> GetSessionStateAsync(CancellationToken cancellationToken = default)
    {
        var result = _store.GetSessionState(_session);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionStateGetSetFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return Task.FromResult(result.Value);
    }

    public override Task SetSessionStateAsync(BinaryData sessionState, CancellationToken cancellationToken = default)
    {
        var result = _store.SetSessionState(_session, sessionState);

        if (!result.IsSuccessful)
        {
            throw ServiceBusClientExceptionFactory.SessionStateGetSetFailed(result.Error.Value, FullyQualifiedNamespace, EntityPath, SessionId);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region Unsupported

    public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object> propertiesToModify, string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task DeferMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<ServiceBusReceivedMessage> ReceiveDeferredMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveDeferredMessagesAsync(IEnumerable<long> sequenceNumbers, CancellationToken cancellationToken = default) => throw ServiceBusClientExceptionFactory.FeatureNotSupported();

    #endregion

}

