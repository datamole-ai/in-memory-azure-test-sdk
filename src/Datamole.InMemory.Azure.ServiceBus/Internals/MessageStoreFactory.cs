namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal static class MessageStoreFactory
{
    public static IMessageStore CreateMessageStore(InMemoryServiceBusEntity entity)
    {
        return entity.SessionEnabled switch
        {
            true => new SessionMessageStore(entity),
            false => new PlainMessageStore(entity)
        };
    }
}
