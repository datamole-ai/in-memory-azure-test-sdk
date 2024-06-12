<h1 align="center">In-memory Test SDK for Azure Service Bus</h1>

<p align="center">This library provides in-memory SDK for Azure Event Hubs which can be used as a drop-in replacement for the official 
<a href="https://www.nuget.org/packages/Azure.Messaging.ServiceBus" target="_blank">Azure.Messaging.ServiceBus</a> in your tests.</p>

<p align="center">
    <a href="#recommended-usage">Recommended Usage</a> |
    <a href="#fault-injection">Fault Injection</a> |
    <a href="#features">Features</a> |
    <a href="#available-client-apis">Available APIs</a> |
    <a href="#available-fluent-assertions">Fluent Assertions</a>
</p>

> [!TIP]
> See the whole [In-Memory Azure Test SDK](../README.md) suite if you are interested in other Azure services.

## Recommended Usage

To get started, add `Datamole.InMemory.Azure.EventHubs` package to your project.

```shell
dotnet add Datamole.InMemory.Azure.ServiceBus
```

Create custom interface for creating the SDK clients.
This interface should be as specific for given use-case as possible to make it simple as possible.
Also, provide default implementation that is creating real Azure SDK clients:

```cs
interface IServiceBusClientFactory
{
    ServiceBusSender CreateSender(string namespaceHostname, string queueOrTopicName);
}

class ServiceBusClientFactory(TokenCredential tokenCredential): IEventHubClientFactory
{
    public ServiceBusSender CreateSender(string namespaceHostname, string queueOrTopicName) => ... ;
}
```

Use this interface to obtain ServiceBus clients in tested code:

```cs
class ExampleService(IServiceBusClientFactory clientFactory)
{
    public async Task SendMessageAsync(string namespaceHostname, string queueName, BinaryData payload)
    {
        var client = clientFactory.CreateSender(namespaceHostname, queueName);

        await client.SendMessageAsync(new ServiceBusMessage(payload));
    }
}
```

Provide implementation of `IServiceBusClientFactory` that is creating the in-memory clients:

```cs
class InMemoryServiceBusClientFactory(InMemoryServiceBusProvider provider) : IServiceBusClientFactory
{
    public ServiceBusSender CreateSender(string namespaceHostname, string queueOrTopicName)
    {
        return new InMemoryServiceBusClient(namespaceHostname, provider).CreateSender(queueOrTopicName);
    }
}
```

When testing, it is now enough to initialize `InMemoryServiceBusProvider` and inject `InMemoryServiceBusClientFactory` to the tested code (e.g. via Dependency Injection):


```csharp
var serviceBusProvider = new InMemoryServiceBusProvider();
var queue = serviceBusProvider.AddNamespace().AddQueue("my-queue");

var services = new ServiceCollection();

services.AddSingleton<ExampleService>();
services.AddSingleton(serviceBusProvider);
services.AddSingleton<IServiceBusClientFactory, InMemoryServiceBusClientFactory>();

var exampleService = services.BuildServiceProvider().GetRequiredService<ExampleService>();

var payload = BinaryData.FromString("test-data");

await exampleService.SendMessageAsync(queue.Namespace.FullyQualifiedNamespace, queue.QueueName, payload);

var receiver = new InMemoryServiceBusClient(queue.Namespace.FullyQualifiedNamespace, serviceBusProvider)
    .CreateReceiver(queue.QueueName);

var message = await receiver.ReceiveMessageAsync();

message.Body.ToString().Should().Be("test-data");
```

## Fault Injection
Fault injections are currently not supported for Azure Service Bus.

## Features

### Supported

* Queues & topics.
* Sessions.
* `PeekLock` & `ReceiveAndDelete` receive modes.
* Client methods explicitly enumerated below are supported and all properties are supported.

### Not Supported

Following features **are not** supported (behavior when using these features is undefined - it might throw an exception or ignore some parameters):

* Deferred messages.
* Scheduled messages.
* Dead-letter queues.
* Processors & rules managers.

## Available Client APIs

### `InMemoryServiceBusClient : ServiceBusClient`

* `CreateSender(...)`
* `CreateReceiver(...)`
* `AcceptNextSessionAsync(...)`
* `AcceptSessionAsync(...)`
* `DisposeAsync(...)`


### `InMemoryServiceBusReceiver : ServiceBusReceiver`

* `ReceiveMessagesAsync(...)`
* `ReceiveMessageAsync()`
* `AbandonMessageAsync(...)`
* `CompleteMessageAsync(...)`
* `RenewMessageLockAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`


### `InMemoryServiceBusSessionReceiver : ServiceBusSessionReceiver`

* `ReceiveMessagesAsync(...)`
* `ReceiveMessageAsync()`
* `AbandonMessageAsync(...)`
* `CompleteMessageAsync(...)`
* `RenewMessageLockAsync(...)`
* `RenewSessionLockAsync(...)`
* `GetSessionStateAsync(...)`
* `SetSessionStateAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`

### `InMemoryServiceBusSender : ServiceBusSender`

* `SendMessageAsync(...)`
* `SendMessagesAsync(...)`
* `CreateMessageBatchAsync(...)`
* `DisposeAsync(...)`
* `CloseAsync(...)`


## Available Fluent Assertions

There are following assertions available for in-memory service bus types:

### `InMemoryServiceBusQueue`

* `BeEmptyAsync()`

### `InMemoryServiceBusTopicSubscription`

* `BeEmptyAsync()`
