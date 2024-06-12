namespace Datamole.InMemory.Azure.Faults;

public record FaultOptions
{
    public int? TransientOccurrenceCount { get; init; }
}

