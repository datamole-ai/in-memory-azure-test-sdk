namespace Datamole.InMemory.Azure.Faults;

public abstract record Fault(FaultOptions? Options)
{
    public Fault() : this((FaultOptions?) null) { }

    public Fault WithTransientOccurrences(int count)
    {
        var options = Options ?? new FaultOptions();

        return this with { Options = options with { TransientOccurrenceCount = count } };
    }
}





