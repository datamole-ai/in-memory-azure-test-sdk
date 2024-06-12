namespace Datamole.InMemory.Azure.ServiceBus;

public record InMemoryServiceBusEntityOptions(bool EnableSessions = false, TimeSpan? LockTime = null);
