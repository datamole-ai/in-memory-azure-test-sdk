using System.Net.Sockets;

using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Clients.Internals;

internal static class ServiceBusClientExceptionFactory
{
    public static NotSupportedException FeatureNotSupported(string? message = null)
    {
        const string baseMessage = "This SDK feature is not supported by in-memory implementation.";

        return message switch
        {
            string => new($"{baseMessage} {message}"),
            null => new(baseMessage),
        };
    }

    public static ServiceBusException MessageLockLost(string fullyQualifiedNamespace, string entityPath)
    {
        return new("Message lock lost.", ServiceBusFailureReason.MessageLockLost, EntityFullPath(fullyQualifiedNamespace, entityPath));
    }

    public static SocketException NamespaceNotFound(string namespaceHostname)
    {
        return new(11001, $"No such host is known: {namespaceHostname}");
    }

    public static ServiceBusException MessagingEntityNotFound(string fullyQualifiedNamespace, string entityPath, string? entitySubpath = null)
    {
        return new("Messaging entity not found.", ServiceBusFailureReason.MessagingEntityNotFound, EntityFullPath(fullyQualifiedNamespace, entityPath, entitySubpath));
    }

    public static ServiceBusException NoSessionAvailable(string fullyQualifiedNamespace, string entityPath, string? entitySubpath = null)
    {
        return new("No session available.", ServiceBusFailureReason.ServiceTimeout, EntityFullPath(fullyQualifiedNamespace, entityPath, entitySubpath));
    }

    public static ServiceBusException SessionNotFound(string fullyQualifiedNamespace, string topicName, string subscriptionName, string sessionId)
    {
        var path = EntityFullPath(fullyQualifiedNamespace, topicName, subscriptionName);
        return SessionNotFoundCore(sessionId, path);
    }


    public static ServiceBusException SessionNotFound(string fullyQualifiedNamespace, string queueName, string sessionId)
    {
        var path = EntityFullPath(fullyQualifiedNamespace, queueName);
        return SessionNotFoundCore(sessionId, path);
    }

    public static ServiceBusException SessionsNotEnabled(string fullyQualifiedNamespace, string entityPath, string? entitySubpath = null)
    {
        return new("Sessions are not enabled.", ServiceBusFailureReason.GeneralError, EntityFullPath(fullyQualifiedNamespace, entityPath, entitySubpath));
    }

    public static ServiceBusException SessionReceiveFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }

    public static Exception SessionRenewFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }

    public static ServiceBusException SessionStateGetSetFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }

    public static ServiceBusException SessionRenewMessageFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            ServiceBusFailureReason.MessageLockLost => MessageLockLost(fullyQualifiedNamespace, entityPath),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }

    public static ServiceBusException SessionCompleteMessageFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            ServiceBusFailureReason.MessageLockLost => MessageLockLost(fullyQualifiedNamespace, entityPath),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }
    public static ServiceBusException SessionAbandonMessageFailed(ServiceBusFailureReason reason, string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return reason switch
        {
            ServiceBusFailureReason.SessionLockLost => SessionLockLost(fullyQualifiedNamespace, entityPath, sessionId),
            _ => throw new InvalidOperationException($"Unexpected failure reason: {reason}"),
        };
    }


    private static ServiceBusException SessionLockLost(string fullyQualifiedNamespace, string entityPath, string sessionId)
    {
        return new($"Session lock lost for session '{sessionId}'.", ServiceBusFailureReason.SessionLockLost, EntityFullPath(fullyQualifiedNamespace, entityPath));
    }

    private static ServiceBusException SessionNotFoundCore(string sessionId, string path)
    {
        return new($"Session {sessionId} not found.", ServiceBusFailureReason.GeneralError, path);
    }

    private static string EntityFullPath(string fullyQualifiedNamespace, string entityPath, string? entitySubpath = null)
    {
        var path = $"{fullyQualifiedNamespace.TrimEnd('/')}/{entityPath}";

        if (entitySubpath is not null)
        {
            path += $"/{entitySubpath}";
        }

        return path;
    }


}
