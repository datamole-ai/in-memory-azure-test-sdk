using System.Security.Cryptography;
using System.Text;

using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace Datamole.InMemory.Azure.Storage.Internals;

internal static class ConnectionStringUtils
{
    public static string GetAccountNameFromConnectionString(string connectionString)
    {
        var options = StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries;

        var pairs = connectionString.Split(';', options);

        var props = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var pair in pairs)
        {
            KeyValuePair<string, string>? kv = pair.Split('=', options) switch
            {
                [var k, var v] => new(k, v),
                _ => null
            };

            if (kv is not null)
            {
                props.Add(kv.Value.Key, kv.Value.Value);
            }
        }

        if (props.TryGetValue("AccountName", out var accountName))
        {
            return accountName;
        }

        if (props.TryGetValue("TableEndpoint", out var tableEndpoint))
        {
            return new TableUriBuilder(new Uri(tableEndpoint)).AccountName;
        }

        if (props.TryGetValue("BlobEndpoint", out var blobEndpoint))
        {
            return new BlobUriBuilder(new Uri(blobEndpoint)).AccountName;
        }

        throw new InvalidOperationException("Storage account name could not be resolved.");

    }

    public static string GetConnectionString(InMemoryStorageAccount account)
    {
        var keySeed = $"{account.Name}|{account.Provider.HostnameSuffix}";

        var key = Convert.ToBase64String(SHA384.HashData(Encoding.UTF8.GetBytes(keySeed)));

        return $"AccountName={account.Name};AccountKey={key};DefaultEndpointsProtocol=https;TableEndpoint={account.TableService.Uri};BlobEndpoint={account.BlobService.Uri}'";
    }
}
