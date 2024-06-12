using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

using Datamole.InMemory.Azure.Faults;
using Datamole.InMemory.Azure.Faults.Internals;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Datamole.InMemory.Azure.Storage.Faults;
using Datamole.InMemory.Azure.Storage.Internals;

namespace Datamole.InMemory.Azure.Storage;

public class InMemoryStorageProvider
{
    private readonly ConcurrentDictionary<string, InMemoryStorageAccount> _storageAccounts = new();

    internal FaultQueue Faults { get; } = new();

    public InMemoryStorageProvider(string? hostnameSuffix = null, TimeProvider? timeProvider = null, ILoggerFactory? loggerFactory = null)
    {
        HostnameSuffix = hostnameSuffix ?? "storage.in-memory.example.com";
        TimeProvider = timeProvider ?? TimeProvider.System;
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    internal TimeProvider TimeProvider { get; }
    internal ILoggerFactory LoggerFactory { get; }
    public string HostnameSuffix { get; }

    public InMemoryStorageAccount AddAccount(string? accountName = null)
    {
        accountName ??= GenerateAccountName();

        var storageAccount = new InMemoryStorageAccount(accountName, this);

        if (!_storageAccounts.TryAdd(accountName, storageAccount))
        {
            throw StorageExceptionFactory.StorageAccountAlreadyExistsFound(accountName);
        }

        return storageAccount;
    }

    private static string GenerateAccountName() => Guid.NewGuid().ToString().Replace("-", string.Empty)[..24];

    public InMemoryStorageAccount GetAccount(string accountName)
    {
        if (!_storageAccounts.TryGetValue(accountName, out var account))
        {
            throw StorageExceptionFactory.StorageAccountNotFound(accountName);
        }

        return account;
    }

    public bool TryGetAccount(string accountName, [NotNullWhen(true)] out InMemoryStorageAccount? account)
    {
        return _storageAccounts.TryGetValue(accountName, out account);
    }

    public IFaultRegistration InjectFault(Func<StorageProviderFaultBuilder, Fault> faultAction, StorageFaultScope? scope = null)
    {
        var fault = faultAction(new(scope ?? new()));
        return Faults.Inject(fault);
    }
}


