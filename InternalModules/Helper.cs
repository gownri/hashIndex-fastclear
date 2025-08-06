using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndexes.InternalModules;

internal static class Helper
{
    internal const int BucketSentinel = 1;
    internal const int BucketFloor = 3;
    internal const int SupportedArraySize = 0x7FFF_FFC0;
    internal const uint CollisionTolerance = Meta.Data.MaxCountableDistance - 32;
    //citation https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int RoundUpToPoolingSize(int size)
    {
        if ((size << 1) <= 0)
            return size;
        size--;
        size |= size >> 1;
        size |= size >> 2;
        size |= size >> 4;
        size |= size >> 8;
        size |= size >> 16;
        return ++size;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetBucketSize(int minimumSize)
        => RoundUpToPoolingSize(minimumSize | BucketFloor);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ManualGetBucketIndex(int bucketSize, int hash)
        => hash & bucketSize - 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetBucketRoom(int size)
    {
        var room = size + (size >> 2) + BucketSentinel;
        return room < 0 ? SupportedArraySize : room;
    }


   
}
