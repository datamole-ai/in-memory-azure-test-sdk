using Azure;
using Azure.Data.Tables;

using Microsoft.Extensions.Time.Testing;

using Datamole.InMemory.Azure.Storage;
using Datamole.InMemory.Azure.Storage.Tables.Clients;

namespace Tests.Storage.Tables;

[TestClass]
public class CrudTests
{
    private class CustomEntity : ITableEntity
    {
        public required string PartitionKey { get; set; }
        public required string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
        public DateTimeOffset? CustomProperty2 { get; set; }
        public long? CustomProperty3 { get; set; }
        public int CustomProperty1 { get; set; }
    }

    [TestMethod]
    public void GetEntity_ForNonExistentEntity_ShouldFail()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var act = () => tableClient.GetEntity<TableEntity>("pk", "rk");

        var exception = act.Should().Throw<RequestFailedException>().Which;

        exception.Status.Should().Be(404);
        exception.ErrorCode.Should().Be("EntityNotFound");

    }

    [TestMethod]
    public void AddEntity_ShouldSucceed()
    {
        var timeProvider = new FakeTimeProvider();

        var account = new InMemoryStorageProvider(timeProvider: timeProvider).AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk") { ["TestProperty"] = 42 };

        tableClient.AddEntity(entity);

        var fetchedEntity = tableClient.GetEntity<TableEntity>("pk", "rk").Value;

        fetchedEntity.GetInt32("TestProperty").Should().Be(42);
        fetchedEntity.ETag.ToString().Should().NotBeNullOrWhiteSpace();
        fetchedEntity.Timestamp.Should().Be(timeProvider.GetUtcNow());
    }

    [TestMethod]
    public void Delete_Existing_Entity_Should_Succeed()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk");

        tableClient.AddEntity(entity);

        tableClient.DeleteEntity("pk", "rk");

        tableClient.Query<TableEntity>().Should().BeEmpty();

    }

    [TestMethod]
    public void Delete_Missing_Entity_Without_ETag_Should_Succeed()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        tableClient.DeleteEntity("pk", "rk");

    }

    [TestMethod]
    public void Delete_Missing_Entity_With_ETag_Should_Fail()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var act = () => tableClient.DeleteEntity("pk", "rk", ifMatch: new ETag("W/Cmawq23"));

        act.Should().Throw<RequestFailedException>().Where(e => e.Status == 404 && e.ErrorCode == "EntityNotFound");

    }

    [TestMethod]
    public void AddEntity_Existing_ShouldFail()
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk");

        tableClient.AddEntity(entity);

        var act = () => tableClient.AddEntity(entity);

        var exception = act.Should().Throw<RequestFailedException>().Which;

        exception.Status.Should().Be(409);
        exception.ErrorCode.Should().Be("EntityAlreadyExists");

    }

    [TestMethod]
    public void Upsert_Existing_Entity_With_Merge_ShouldSucceed()
    {
        var timeProvider = new FakeTimeProvider();

        var account = new InMemoryStorageProvider(timeProvider: timeProvider).AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity1 = new TableEntity("pk", "rk") { ["TestProperty1"] = 11, ["TestProperty2"] = 12 };
        var entity2 = new TableEntity("pk", "rk") { ["TestProperty1"] = 21, ["TestProperty3"] = 23 };

        tableClient.UpsertEntity(entity1, TableUpdateMode.Merge);
        tableClient.UpsertEntity(entity2, TableUpdateMode.Merge);

        var fetchedEntity = tableClient.GetEntity<TableEntity>("pk", "rk").Value;


        fetchedEntity.GetInt32("TestProperty1").Should().Be(21);
        fetchedEntity.GetInt32("TestProperty2").Should().Be(12);
        fetchedEntity.GetInt32("TestProperty3").Should().Be(23);

    }

    [TestMethod]
    public void Upsert_Existing_Entity_With_Replace_ShouldSucceed()
    {
        var timeProvider = new FakeTimeProvider();

        var account = new InMemoryStorageProvider(timeProvider: timeProvider).AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity1 = new TableEntity("pk", "rk") { ["TestProperty1"] = 11, ["TestProperty2"] = 12 };
        var entity2 = new TableEntity("pk", "rk") { ["TestProperty1"] = 21, ["TestProperty3"] = 23 };

        tableClient.UpsertEntity(entity1, TableUpdateMode.Replace);
        tableClient.UpsertEntity(entity2, TableUpdateMode.Replace);

        var fetchedEntity = tableClient.GetEntity<TableEntity>("pk", "rk").Value;


        fetchedEntity.GetInt32("TestProperty1").Should().Be(21);
        fetchedEntity.GetInt32("TestProperty2").Should().BeNull();
        fetchedEntity.GetInt32("TestProperty3").Should().Be(23);

    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void Upsert_New_Entity_Should_Succeed(TableUpdateMode updateMode)
    {
        var timeProvider = new FakeTimeProvider();

        var account = new InMemoryStorageProvider(timeProvider: timeProvider).AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk") { ["TestProperty"] = 42 };

        tableClient.UpsertEntity(entity, updateMode);

        var fetchedEntity = tableClient.GetEntity<TableEntity>("pk", "rk").Value;

        fetchedEntity.ETag.ToString().Should().NotBeNullOrWhiteSpace();
        fetchedEntity.GetInt32("TestProperty").Should().Be(42);
        fetchedEntity.Timestamp.Should().Be(timeProvider.GetUtcNow());
    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void UpsertEntity_OfCustomType_ShouldSucceed(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var now = DateTimeOffset.UtcNow;

        var entity = new CustomEntity
        {
            PartitionKey = "pk",
            RowKey = "rk",
            CustomProperty1 = 42,
            CustomProperty2 = now,
            CustomProperty3 = 4242
        };

        tableClient.UpsertEntity(entity, updateMode);

        var fetchedEntity = tableClient.GetEntity<CustomEntity>("pk", "rk").Value;

        fetchedEntity.ETag.ToString().Should().NotBeNullOrWhiteSpace();
        fetchedEntity.CustomProperty1.Should().Be(42);
        fetchedEntity.CustomProperty2.Should().Be(now);
        fetchedEntity.CustomProperty3.Should().Be(4242);
    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void UpsertEntity_ShouldChangeETag(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity1 = new TableEntity("pk", "rk") { ["TestProperty"] = 42 };

        tableClient.AddEntity(entity1);

        var entity2 = tableClient.GetEntity<TableEntity>("pk", "rk").Value;

        entity2["TestProperty"] = 43;

        tableClient.UpsertEntity(entity2, updateMode);

        var entity3 = tableClient.GetEntity<TableEntity>("pk", "rk").Value;

        entity3.GetInt32("TestProperty").Should().Be(43);
        entity3.ETag.Should().NotBe(entity2.ETag);

    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void UpsertEntity_ExistingEntity_WithoutETag_ShouldSucceeed(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity1 = new TableEntity("pk", "rk");
        var entity2 = new TableEntity("pk", "rk");

        tableClient.UpsertEntity(entity1, updateMode);
        tableClient.UpsertEntity(entity2, updateMode);

    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void UpsertEntity_ExistingEntity_WithInvalidETag_ShouldFail(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity1 = new TableEntity("pk", "rk");
        var entity2 = new TableEntity("pk", "rk") { ETag = new(Guid.NewGuid().ToString()) };

        tableClient.UpsertEntity(entity1, updateMode);

        var act = () => tableClient.UpsertEntity(entity2, updateMode);

        var exception = act.Should().Throw<RequestFailedException>().Which;

        exception.Status.Should().Be(412);
        exception.ErrorCode.Should().Be("UpdateConditionNotSatisfied");
    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void UpsertEntity_ForNonExistingEntity_WithETag_ShouldFail(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk") { ETag = new(Guid.NewGuid().ToString()) };

        var act = () => tableClient.UpsertEntity(entity, updateMode);

        var exception = act.Should().Throw<RequestFailedException>().Which;

        exception.Status.Should().Be(412);
        exception.ErrorCode.Should().Be("UpdateConditionNotSatisfied");
    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void Update_Existing_Entity_Should_Succeed(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk") { ["property1"] = 41 };

        var addResponse = tableClient.AddEntity(entity);

        var eTagBeforeUpdate = addResponse.Headers.ETag;

        eTagBeforeUpdate.Should().NotBeNull();

        entity["property1"] = 42;

        var updateResponse = tableClient.UpdateEntity(entity, eTagBeforeUpdate!.Value, updateMode);

        var eTagAfterUpdate = updateResponse.Headers.ETag;

        eTagAfterUpdate.Should().NotBeNull();
        eTagAfterUpdate.Should().NotBe(eTagBeforeUpdate);

        var fetchedEntity = tableClient.GetEntity<TableEntity>("pk", "rk").Value;

        fetchedEntity.GetInt32("property1").Should().Be(42);

    }

    [TestMethod]
    [DataRow(TableUpdateMode.Replace, DisplayName = "Replace")]
    [DataRow(TableUpdateMode.Merge, DisplayName = "Merge")]
    public void Update_NonExistingEntity_ShouldFail(TableUpdateMode updateMode)
    {
        var account = new InMemoryStorageProvider().AddAccount();

        var tableClient = InMemoryTableClient.FromAccount(account, "TestTable");

        tableClient.Create();

        var entity = new TableEntity("pk", "rk") { ETag = new(Guid.NewGuid().ToString()) };

        var act = () => tableClient.UpdateEntity(entity, entity.ETag, updateMode);

        var exception = act.Should().Throw<RequestFailedException>().Which;

        exception.Status.Should().Be(404);
        exception.ErrorCode.Should().Be("EntityNotFound");
    }
}
