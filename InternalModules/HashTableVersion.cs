using System.Runtime.CompilerServices;

namespace HashIndex.InternalModules;

internal readonly struct HashTableVersion
{
    private const int hashTableMask = ushort.MaxValue;
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
    public readonly ushort HashTable
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
    public static HashTableVersion Create()
    {
        return new(0, 1);
    }
    private HashTableVersion(short generation, ushort table)
        => this.value = (uint)generation << generationOffset | table;
    private HashTableVersion(uint value)
        => this.value = value;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public HashTableVersion ReuseTable(HashTableVersion tableVersion)
        => new(unchecked(this.value & (uint)(~hashTableMask) | tableVersion.HashTable));
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly HashTableVersion IncrementTable(out bool isOverflow)
    {
        const int increment = 1;
        const int overflowedIncrement = 3;
        var isOverflowL = (this.value & hashTableMask) == overflowThreshold;
        isOverflow = isOverflowL;
        return new((uint)(this.value 
            + (isOverflowL 
                ? overflowedIncrement
                : increment
            )
        ));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly HashTableVersion ResetGeneration()
        => new(0, this.HashTable);

    public readonly override string ToString()
        => $"Gen:{this.Generation,5} Bkt:{this.HashTable,5}";
}
