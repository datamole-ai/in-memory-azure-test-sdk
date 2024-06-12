namespace Datamole.InMemory.Azure.EventHubs.Clients.Internals;


internal readonly record struct Position(int SequenceNumber, bool IsInclusive)
{
    public static Position FromSequenceNumber(long sequenceNumber, bool isInclusive) => new((int) sequenceNumber, isInclusive);
}
