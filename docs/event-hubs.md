<h1 align="center">In-memory Test SDK for Azure Event Hubs</h1>

<p align="center">This library provides in-memory SDK for Azure Event Hubs which can be used as a drop-in replacement for the official 
<a href="https://www.nuget.org/packages/Azure.Messaging.EventHubs" target="_blank">Azure.Messaging.EventHubs SDK</a> in your tests.</p>

<p align="center">
    <a href="#recommended-usage">Recommended Usage</a> |
    <a href="#fault-injection">Fault Injection</a> |
    <a href="#features">Features</a> |
    <a href="#available-client-apis">Available APIs</a>
</p>

> [!TIP]
> See the whole [In-Memory Azure Test SDK](../README.md) suite if you are interested in other Azure services.



## Recommended Usage

To get started, add `Datamole.InMemory.Azure.EventHubs` package to your project.

```shell
dotnet add Datamole.InMemory.Azure.EventHubs
```

Create custom interface for creating the SDK clients. 
This interface should be as specific for given use-case as possible to make it simple as possible. 
Also, provide default implementation that is creating real Azure SDK clients:

```cs
interface IEventHubClientFactory
{
    EventHubProducerClient CreateProducerClient(string namespaceHostname, string eventHubName);
}

class EventHubClientFactory(TokenCredential tokenCredential): IEventHubClientFactory
{
    public EventHubProducerClient CreateProducerClient(string namespaceHostname, string eventHubName) => ... ;
}
```

Use this interface to obtain EventHub clients in tested code:

```cs
class ExampleService(IEventHubClientFactory clientFactory)
{
    public async Task SendEventAsync(string namespaceHostname, string eventHubName, BinaryData payload)
    {
        var client = clientFactory.CreateProducerClient(namespaceHostname, eventHubName);

        await client.SendAsync(new EventData(payload));
    }
}
```

Provide implementation of `IEventHubClientFactory` that is creating the in-memory clients:

```cs
class InMemoryEventHubClientFactory(InMemoryEventHubProvider provider): IEventHubClientFactory
{
    public EventHubProducerClient CreateProducerClient(string namespaceHostname, string eventHubName)
    {
        return new InMemoryEventHubProducerClient(namespaceHostname, eventHubName, provider);
    }
}
```

When testing, it is now enough to initialize `InMemoryEventHubProvider` and inject `InMemoryEventHubClientFactory` to the tested code (e.g. via Dependency Injection):

```cs
var eventHubProvider = new InMemoryEventHubProvider();
var eventHub = eventHubProvider.AddNamespace().AddEventHub("test-event-hub", numberOfPartitions: 4);

var services = new ServiceCollection();

services.AddSingleton<ExampleService>();
services.AddSingleton(eventHubProvider);
services.AddSingleton<IEventHubClientFactory, InMemoryEventHubClientFactory>();

var exampleService = services.BuildServiceProvider().GetRequiredService<ExampleService>();

var payload = BinaryData.FromString("test-data");

await exampleService.SendEventAsync(eventHub.Namespace.Hostname, eventHub.Name, payload);

var receiver = InMemoryPartitionReceiver.FromEventHub(partitionId: "0", eventHub);

var batch = await receiver.ReceiveBatchAsync(100, TimeSpan.Zero);

batch.Should().ContainSingle(e => e.EventBody.ToString() == "test-data");
```

## Fault Injection

Fault injections allows you to simulate transient and persistent faults in the Event Hub client. 
Thanks to that you can test how your application behaves in case of Azure outages, network issues, timeouts, etc.


To inject a fault, you must call the `InjectFault` method on an `InMemoryEventHubProvider`, `InMemoryEventHubNamespace`, or `InMemoryEventHub` instance.
The first argument lets you define the type of the fault, and optionally its scope and the number of occurrences it should be active.

See the following example that simulates the "ServiceBusy" fault during the first call to the `ReceiveBatchAsync` method:
```cs
var eventHub = new InMemoryEventHubProvider().AddNamespace().AddEventHub("test-eh", 2);

eventHub.InjectFault(
    faultBuilder => faultBuilder
        .ServiceIsBusy()
        .WithScope(scope => scope with {Operation = EventHubOperation.ReceiveBatch})
        .WithTransientOccurrences(1));

var receiver = InMemoryPartitionReceiver.FromEventHub("0", eventHub);

var act = () => receiver.ReceiveBatchAsync(10, TimeSpan.FromMilliseconds(10));

await act
    .Should()
    .ThrowAsync<EventHubsException>()
    .Where(e => e.Reason == EventHubsException.FailureReason.ServiceBusy);

await act.Should().NotThrowAsync();
```

### Persistent Faults
You can make the injected faults persistent simply by omitting the `WithTransientOccurrences` method call.
In this case, the fault will be active until you call the `Resolve` method on the `IFaultRegistration` instance which is returned by the `InjectFault` method.

```cs
var eventHub = new InMemoryEventHubProvider().AddNamespace().AddEventHub("test-eh", 2);

var fault = eventHub.InjectFault(faults => faults.ServiceIsBusy());

var receiver = InMemoryPartitionReceiver.FromEventHub("0", eventHub);

var act = () => receiver.ReceiveBatchAsync(10, TimeSpan.FromMilliseconds(10));

await act
    .Should()
    .ThrowAsync<EventHubsException>()
    .Where(e => e.Reason == EventHubsException.FailureReason.ServiceBusy);

fault.Resolve();

await act.Should().NotThrowAsync();
```

## Features

### Supported
* All public client properties.
* Client methods explicitly enumerated below.
* Partition Keys.
* Message Id, Correlation Id, Content Type.
* Sequence number based starting positions (including `Earliest` and `Latest`).
* Randomization of initial sequence numbers for event hub partitions.

### Not Supported
Following features **are not** supported (behavior when using these features is undefined - it might throw an exception or ignore some parameters):

* Offset based starting positions.

## Available Client APIs

### `InMemoryEventHubConsumerClient : EventHubConsumerClient`

* `GetEventHubPropertiesAsync(...)`
* `GetPartitionIdsAsync(...)`
* `GetPartitionPropertiesAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`


### `InMemoryPartitionReceiver : PartitionReceiver`

* `GetPartitionPropertiesAsync(...)`
* `ReadLastEnqueuedEventProperties()`
* `ReceiveBatchAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`


### `InMemoryEventHubProducerClient : EventHubProducerClient`

* `GetEventHubPropertiesAsync(...)`
* `GetPartitionIdsAsync(...)`
* `GetPartitionPropertiesAsync(...)`
* `CreateBatchAsync(...)`
* `SendAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`
