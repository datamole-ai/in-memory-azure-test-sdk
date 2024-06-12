using Azure;

using Datamole.InMemory.Azure.Faults;

namespace Datamole.InMemory.Azure.Storage.Faults;

public abstract record StorageFault() : Fault();

public abstract record StorageFault<TScope>(TScope Scope) : StorageFault where TScope : class
{
    public abstract RequestFailedException CreateException(TScope currentScope);

    public StorageFault<TScope> WithScope(Func<TScope, TScope> scopeFunc)
    {
        return this with { Scope = scopeFunc(Scope) };
    }
}
