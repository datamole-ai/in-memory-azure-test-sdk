namespace Datamole.InMemory.Azure.ServiceBus.Internals;

internal interface IEntityIdentity
{
    string FullyQualifiedNamespace { get; }
    string EntityPath { get; }
}
