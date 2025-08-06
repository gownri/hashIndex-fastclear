using System.Runtime.CompilerServices;

namespace HashIndexes.InternalModules;

internal readonly struct BucketVersion
{
    private const int bucketMask = ushort.MaxValue;
    private const int overflowThreshold = ushort.MaxValue - 1;
    private const int generationOffset = sizeof(ushort) * 8;
    private const int generationUnit = 1 << generationOffset;
    private const uint maxGenerationCount = (uint)ushort.MaxValue << generationOffset;
    private readonly uint value;
    public readonly bool IsBoundaryGeneration => this.value >= maxGenerationCount;
    internal readonly uint RawValue
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => this.value;
    }
    public readonly ushort Bucket
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => unchecked((ushort)this.value);
    }
    public readonly ushort Generation
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort)(this.value >> generationOffset);
    }

    public readonly bool IsValid => this.value != 0;
    public static BucketVersion Create()
    {
        return new(0, 1);
    }
    private BucketVersion(short generation, ushort bucket)
        => this.value = (uint)generation << generationOffset | bucket;
    private BucketVersion(uint value)
        => this.value = value;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BucketVersion ReuseBucket(BucketVersion bucketVersion)
        => new(unchecked(this.value & (uint)(~bucketMask) | bucketVersion.Bucket));
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly BucketVersion IncrementBucket(out bool isOverflow)
    {
        const int increment = 1;
        const int overflowedIncrement = 3;
        var isOverflowL = (this.value & bucketMask) == overflowThreshold;
        isOverflow = isOverflowL;
        return new((uint)(this.value 
            + (isOverflowL 
                ? overflowedIncrement
                : increment
            )
        ));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly BucketVersion ResetGeneration()
        => new(0, this.Bucket);
}
