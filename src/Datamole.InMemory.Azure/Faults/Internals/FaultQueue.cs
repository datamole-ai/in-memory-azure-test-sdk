using System.Diagnostics.CodeAnalysis;

namespace Datamole.InMemory.Azure.Faults.Internals;

internal class FaultQueue
{
    private readonly LinkedList<FaultRegistration> _faults = new();

    public bool TryGetFault<T>(Func<T, bool> filter, [NotNullWhen(true)] out T? result) where T : Fault
    {
        lock (_faults)
        {
            var node = _faults.First;

            while (node is not null)
            {
                var faultRegistration = node.Value;

                if (faultRegistration.Fault is T fault && filter(fault))
                {
                    var occurenceCount = faultRegistration.IncrementOccurenceCount();

                    if (faultRegistration.Options.TransientOccurrenceCount is int transientOccurrenceCount)
                    {
                        if (transientOccurrenceCount == occurenceCount)
                        {
                            node.List?.Remove(node);
                        }
                    }

                    result = fault;
                    return true;
                }

                node = node.Next;

            }

            result = null;
            return false;

        }
    }

    internal FaultRegistration Inject<T>(T fault) where T : Fault
    {
        lock (_faults)
        {
            var faultRegistration = new FaultRegistration(fault);
            var node = _faults.AddLast(faultRegistration);

            faultRegistration.UnregisterAction = () => UnregisterFault(node);

            return faultRegistration;
        }
    }

    private void UnregisterFault(LinkedListNode<FaultRegistration> node)
    {
        lock (_faults)
        {
            node.List?.Remove(node);
        }
    }

}
