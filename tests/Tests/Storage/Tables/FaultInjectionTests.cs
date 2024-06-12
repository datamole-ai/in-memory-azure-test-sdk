using Azure;
using Azure.Data.Tables;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Tables.Clients;

namespace Tests.Storage.Tables;

[TestClass]
public class FaultInjectionTests
{

    [TestMethod]
    public void InjectedPersistentFault_ShouldBeResolved()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var fault = account.InjectFault(faults => faults.ForTableService().ServiceIsBusy());

        var act = () => tableClient.Query<TableEntity>(e => e.PartitionKey == "abc");

        act.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");

        fault.Resolve();

        act.Should().NotThrow();
    }

    [TestMethod]
    public void InjectedTransientFault_ShouldBeResolved()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();


        var fault = account.InjectFault(faults => faults
            .ForTableService()
            .ServiceIsBusy()
            .WithTransientOccurrences(2));


        var act = () => tableClient.Query<TableEntity>(e => e.PartitionKey == "abc");

        act.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");

        act.Should().Throw<RequestFailedException>().Which.ErrorCode.Should().Be("ServerBusy");

        act.Should().NotThrow();
    }



}
