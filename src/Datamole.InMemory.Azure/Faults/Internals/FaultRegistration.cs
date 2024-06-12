namespace Datamole.InMemory.Azure.Faults.Internals;

internal class FaultRegistration : IFaultRegistration
{
    private int _occurenceCount;

    public Fault Fault { get; }
    public FaultOptions Options { get; }

    public int IncrementOccurenceCount() => Interlocked.Increment(ref _occurenceCount);

    public FaultRegistration(Fault fault)
    {
        Fault = fault;
        _occurenceCount = 0;
        Options = fault.Options ?? new();
    }

    public Action? UnregisterAction { get; set; }

    public void Resolve() => UnregisterAction?.Invoke();

}
