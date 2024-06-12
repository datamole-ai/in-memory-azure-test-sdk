using Azure.Messaging.EventHubs;

using Datamole.InMemory.Azure.EventHubs;

namespace Tests.EventHub;

[TestClass]
public class EventHubNamespaceTests
{
    [TestMethod]
    public void ConnectionString_ShouldBeReturned()
    {
        var eventHubNamespace = new InMemoryEventHubProvider().AddNamespace();
        var connectionString = eventHubNamespace.GetConnectionString();

        var connection = new EventHubConnection(connectionString, "test-eh");

        connection.FullyQualifiedNamespace.Should().Be(eventHubNamespace.Hostname);
        connection.EventHubName.Should().Be("test-eh");
    }
}
