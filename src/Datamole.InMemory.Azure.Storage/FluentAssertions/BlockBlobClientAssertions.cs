using System.Diagnostics;

using Azure;
using Azure.Storage.Blobs.Specialized;

using FluentAssertions.Execution;
using FluentAssertions.Primitives;

namespace Datamole.InMemory.Azure.Storage.FluentAssertions;

public class BlockBlobClientAssertions : ReferenceTypeAssertions<BlobBaseClient, BlockBlobClientAssertions>
{
    public BlockBlobClientAssertions(BlobBaseClient subject) : base(subject) { }

    protected override string Identifier => nameof(BlobBaseClient);

    public async Task ExistAsync(TimeSpan? maxWaitTime = null, string? because = null, params object[] becauseArgs)
    {
        maxWaitTime ??= TimeSpan.FromSeconds(8);

        var startTime = Stopwatch.GetTimestamp();

        while (Stopwatch.GetElapsedTime(startTime) < maxWaitTime)
        {
            try
            {
                if (await Subject.ExistsAsync())
                {
                    return;
                }
            }
            catch (RequestFailedException) { }

            await Task.Delay(10);
        }

        Execute
            .Assertion
            .BecauseOf(because, becauseArgs)
            .FailWith("Blob {0} should exist{reason} but it does not exist event after {1} seconds.", Subject.Uri, maxWaitTime.Value.TotalSeconds);
    }
}
