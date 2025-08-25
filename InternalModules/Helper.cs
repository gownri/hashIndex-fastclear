using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndex.InternalModules;

internal static class Helper
{
    internal const int KeysFloor = 4;
    internal const int TableFloor = 15;
    internal const int SupportedArraySize = 0x7FFF_FFC0;
    internal const uint CollisionTolerance = 100;
    internal const int InvalidIndex = -1;

    private static readonly Meta invalidRef = new Meta(-1, Meta.Data.Sentinel);
    internal static ref Meta InvalidMetaRef
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref Unsafe.AsRef(invalidRef);
    }

    //citation https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int RoundUpToTableSize(int size)
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
    internal static int GetTableSize(int minimumSize)
        => RoundUpToTableSize(minimumSize | TableFloor);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ComputeHashTableRoom(int size)
        => (size + (size >> 1)) | TableFloor;


   
}
