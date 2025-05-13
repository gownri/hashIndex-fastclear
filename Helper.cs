using System.Runtime.CompilerServices;

namespace HashIndexes
{
    internal static class Helper
    {
        internal const int BucketSentinel = 1;

        //citation https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
        internal static int GetNextPowerOfTwo(int size)
        {
            //floor 2
            if (size <= 2)
                return 2;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int BucketRoom(int size)
            => size + (size >> 1) + Helper.BucketSentinel;
    }
}
