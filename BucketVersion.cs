using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace HashIndexers
{
    internal readonly struct BucketVersion
    {
        private const int bucketMask = ushort.MaxValue;
        private const int generationOffset = sizeof(ushort) * 8;
        private const int generationUnit = 1 << generationOffset;

        private readonly uint value;
        public readonly ushort Bucket
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => unchecked((ushort)this.value);
        }
        public readonly int Generation
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(this.value >> generationOffset);
        }

        public readonly bool IsInvalid => this.value == 0;
        public static BucketVersion Create()
        {
            return new(1, 1);
        }
        private BucketVersion(short generation, ushort bucket)
            => this.value = ((uint)generation << generationOffset) | (uint)bucket;
        private BucketVersion(uint value)
            => this.value = value;

        public readonly BucketVersion IncrementBucket(out bool isOverflow)
        {
            isOverflow = (this.value & bucketMask) == bucketMask;
            return new((uint)(this.value + (isOverflow ? 2 : 1)));
        }
        public readonly BucketVersion IncrementGeneration()
            => new(this.value + generationUnit);
    }
}
