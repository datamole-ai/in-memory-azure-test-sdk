namespace Datamole.InMemory.Azure.EventHubs.Clients.Internals;

internal readonly record struct StartingPosition(int SequenceNumber, bool IsInclusive, bool IsEarliest, bool IsLatest)
{
    public static StartingPosition FromSequenceNumber(long sequenceNumber, bool isInclusive)
    {
        return new((int) sequenceNumber, isInclusive, false, false);
    }

    public static StartingPosition Earliest => new(-1, false, true, false);
    public static StartingPosition Latest => new(-1, false, false, true);
}
