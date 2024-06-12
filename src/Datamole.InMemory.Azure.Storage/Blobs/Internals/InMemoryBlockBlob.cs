using System.Diagnostics.CodeAnalysis;

using Azure;
using Azure.Storage.Blobs.Models;

namespace Datamole.InMemory.Azure.Storage.Blobs.Internals;

internal class InMemoryBlockBlob
{
    private readonly object _lock = new();

    private Dictionary<string, Block>? _uncommittedBlocks;
    private List<Block>? _committedBlocks;
    private BinaryData? _cachedContent;
    private BlobProperties _properties;

    public InMemoryBlockBlob(string blobName, InMemoryBlobContainer container)
    {
        Name = blobName;
        Container = container;
        _cachedContent = null;
        _properties = BlobsModelFactory.BlobProperties(
            eTag: new ETag(Guid.NewGuid().ToString()),
            lastModified: DateTimeOffset.UtcNow);

    }

    public BlobType BlobType => BlobType.Block;

    public string Name { get; }
    public InMemoryBlobContainer Container { get; }

    public BlobProperties GetProperties()
    {
        lock (_lock)
        {
            return _properties;
        }
    }

    public BinaryData GetContent()
    {
        lock (_lock)
        {
            return GetContentUnsafe();
        }
    }

    public bool HasCommitedBlockList()
    {
        lock (_lock)
        {
            return _committedBlocks is not null;
        }
    }

    public BlobItem AsBlobItem() => BlobsModelFactory.BlobItem(Name);

    public BlobDownloadInfo GetDownloadInfo()
    {
        var content = GetContent();

        var properties = GetProperties();

        return BlobsModelFactory.BlobDownloadInfo(
            blobType: BlobType,
            contentLength: content.GetLenght(),
            eTag: properties.ETag,
            lastModified: properties.LastModified,
            content: content.ToStream()
            );
    }

    public BlobContentInfo GetContentInfo()
    {
        var properties = GetProperties();

        return BlobsModelFactory.BlobContentInfo(properties.ETag, properties.LastModified, default, default, default, default, default);
    }

    public BlobDownloadDetails GetDownloadDetails() => GetDownloadInfo().Details;

    public BlobDownloadStreamingResult GetStreamingDownloadResult()
    {
        return BlobsModelFactory.BlobDownloadStreamingResult(GetContent().ToStream(), GetDownloadDetails());
    }

    public BlobDownloadResult GetDownloadResult()
    {
        return BlobsModelFactory.BlobDownloadResult(GetContent(), GetDownloadDetails());
    }

    public override string? ToString() => $"{Container} / {Name}";


    public bool TryCommitBlockList(
        IEnumerable<string> base64BlockIds,
        BlobHttpHeaders? headers,
        IDictionary<string, string>? metadata,
        [NotNullWhen(false)] out CommitBlockListError? error)
    {
        lock (_lock)
        {
            IReadOnlyDictionary<string, Block>? currentUncommittedBlocks = _uncommittedBlocks;
            IReadOnlyDictionary<string, Block>? currentCommittedBlocks = _committedBlocks?.ToDictionary(b => b.Id);

            var stagingCommittedBlocks = new List<Block>();

            foreach (var id in base64BlockIds)
            {
                if (currentUncommittedBlocks is not null)
                {
                    if (currentUncommittedBlocks.TryGetValue(id, out var block))
                    {
                        stagingCommittedBlocks.Add(block);
                        continue;
                    }
                }

                if (currentCommittedBlocks is not null)
                {
                    if (currentCommittedBlocks.TryGetValue(id, out var block))
                    {
                        stagingCommittedBlocks.Add(block);
                        continue;
                    }
                }

                error = new CommitBlockListError.BlockNotFound(id);
                return false;
            }

            if (stagingCommittedBlocks.Count > InMemoryBlobService.MaxBlockCount)
            {
                error = new CommitBlockListError.BlockCountExceeded(InMemoryBlobService.MaxBlockCount, stagingCommittedBlocks.Count);
                return false;
            }

            _cachedContent = null;
            _uncommittedBlocks = null;
            _committedBlocks = stagingCommittedBlocks;

            SetPropertiesUnsafe(headers, metadata);
        }

        error = null;
        return true;

    }


    public bool TryStageBlock(
        string base64BlockId,
        BinaryData content,
        [NotNullWhen(true)] out Block? result,
        [NotNullWhen(false)] out StageBlockError? error)
    {

        if (content.GetLenght() > InMemoryBlobService.MaxBlockSize)
        {
            result = null;
            error = new StageBlockError.BlockTooLarge(InMemoryBlobService.MaxBlockSize, content.GetLenght());
            return false;
        }

        var block = new Block(base64BlockId, content);

        lock (_lock)
        {
            _uncommittedBlocks ??= new();

            if (_uncommittedBlocks.Count >= InMemoryBlobService.MaxUncommitedBlocks)
            {
                result = null;
                error = new StageBlockError.TooManyUncommittedBlocks(InMemoryBlobService.MaxUncommitedBlocks, _uncommittedBlocks.Count);
                return false;
            }

            _uncommittedBlocks[base64BlockId] = block;

            result = block;
            error = null;
            return true;

        }
    }



    public BlockList GetBlockList(BlockListTypes blockListTypes)
    {
        IEnumerable<BlobBlock>? commitedBlocks = null;
        IEnumerable<BlobBlock>? uncommittedBlocks = null;

        if (blockListTypes.HasFlag(BlockListTypes.Committed))
        {
            commitedBlocks = _committedBlocks?.Select(b => BlobsModelFactory.BlobBlock(b.Id, b.Content.GetLenght()));
            commitedBlocks ??= Enumerable.Empty<BlobBlock>();
        }

        if (blockListTypes.HasFlag(BlockListTypes.Uncommitted))
        {
            uncommittedBlocks = _uncommittedBlocks?.Values.Select(b => BlobsModelFactory.BlobBlock(b.Id, b.Content.GetLenght()));
            uncommittedBlocks ??= Enumerable.Empty<BlobBlock>();
        }

        return BlobsModelFactory.BlockList(commitedBlocks, uncommittedBlocks);
    }

    private BinaryData GetContentUnsafe()
    {
        if (_committedBlocks is null)
        {
            throw new InvalidOperationException($"Blob '{Name}' in container '{Container}' has no content.");
        }

        if (_cachedContent is not null)
        {
            return _cachedContent;
        }

        var len = _committedBlocks.Sum(b => b.Content.GetLenght());

        Memory<byte> buffer = new byte[len];

        var bufferIndex = 0;

        foreach (var block in _committedBlocks)
        {
            block.Content.ToMemory().CopyTo(buffer[bufferIndex..]);
            bufferIndex += block.Content.GetLenght();
        }

        return _cachedContent = new(buffer);
    }




    [MemberNotNull(nameof(_properties))]
    private void SetPropertiesUnsafe(BlobHttpHeaders? headers, IDictionary<string, string>? metadata)
    {
        var newProperties = BlobsModelFactory.BlobProperties(
            contentLength: _committedBlocks is null ? 0 : GetContentUnsafe().ToMemory().Length,
            metadata: metadata ?? _properties?.Metadata,
            eTag: new ETag(Guid.NewGuid().ToString()),
            lastModified: DateTimeOffset.UtcNow,
            contentType: headers?.ContentType ?? _properties?.ContentType,
            contentEncoding: headers?.ContentEncoding ?? _properties?.ContentEncoding
            );

        _properties = newProperties;
    }


    public record Block(string Id, BinaryData Content)
    {
        public BlockInfo GetInfo() => BlobsModelFactory.BlockInfo(null, null, null);
    }

    public abstract record CommitBlockListError
    {
        public record BlockCountExceeded(int Limit, int ActualCount) : CommitBlockListError;
        public record BlockNotFound(string BlockId) : CommitBlockListError;
    }

    public abstract record StageBlockError
    {
        public record TooManyUncommittedBlocks(int Limit, int ActualCount) : StageBlockError;
        public record BlockTooLarge(int Limit, int ActualSize) : StageBlockError;
    }



}

