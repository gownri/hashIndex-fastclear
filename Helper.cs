using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace HashIndexers
{
    internal static class Helper
    {
        internal const int Sentinel = 1;

        //citation https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
        internal static int GetNextPowerOfTwo(int size)
        {
            if (size <= 0)
                return 0;
            size |= size >> 1;
            size |= size >> 2;
            size |= size >> 4;
            size |= size >> 8;
            size |= size >> 16;
            return ++size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ManualGetBucketIndex(int hash, int bucketSize)
            => hash & (bucketSize - 1);
    }
}
