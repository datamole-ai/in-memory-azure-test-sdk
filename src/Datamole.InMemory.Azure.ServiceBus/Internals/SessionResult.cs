using System.Diagnostics.CodeAnalysis;

using Azure.Messaging.ServiceBus;

namespace Datamole.InMemory.Azure.ServiceBus.Internals;

public class SessionResult<T> where T : notnull
{
    [MemberNotNullWhen(true, nameof(Value))]
    [MemberNotNullWhen(false, nameof(Error))]
    public bool IsSuccessful => Value is not null;

    public SessionResult(ServiceBusFailureReason error)
    {
        Error = error;
    }

    public SessionResult(T value)
    {
        Value = value;
    }

    public ServiceBusFailureReason? Error { get; }
    public T? Value { get; }
}

public class SessionResult
{
    [MemberNotNullWhen(false, nameof(Error))]
    public bool IsSuccessful => Error is null;

    public static SessionResult Successful { get; } = new();

    public SessionResult(ServiceBusFailureReason error)
    {
        Error = error;
    }

    private SessionResult()
    {
    }

    public ServiceBusFailureReason? Error { get; }

}
