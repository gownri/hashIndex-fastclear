using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
namespace HashIndexers
{
    public ref struct UnitHashIndex
    {
        internal readonly Span<Meta> bucket;
        internal readonly Span<Meta> BucketSource => this.bucket;
        internal readonly BucketVersion Version => this.version;
        private int count;
        private BucketVersion version;

        public readonly int BucketSize => this.bucket.Length;
        public readonly int Count => this.count;

        public readonly bool IsAddable => this.count < (this.bucket.Length - Helper.Sentinel);

        public static int SizeOfMeta => Unsafe.SizeOf<Meta>();
        public static int ComputeBufferByteSize(int bucketCapacity)
            => Helper.GetNextPowerOfTwo(bucketCapacity)*Unsafe.SizeOf<Meta>();
        public UnitHashIndex(Span<byte> bufferBucket, bool isCleanBuffer)
        {
            var bucket = MemoryMarshal.Cast<byte, Meta>(bufferBucket);
            if(!isCleanBuffer)
                bucket.Clear();
            var lsb = bucket.Length & (-bucket.Length);
            this.bucket = lsb != bucket.Length 
                ? Slice(bucket, lsb) 
                : bucket;
            this.version = BucketVersion.Create();
            this.count = 0;

            static Span<Meta> Slice(Span<Meta> span, int lsb)
            {
                uint max = 0;
                for (max = (uint)lsb; max < span.Length; max <<= 1) ;
                return span.Slice(0, (int)(max >> 1));
            }
        }

        public UnitHashIndex(BufferPack bufferPack) : this(bufferPack.Rent(), bufferPack.version)
        { }
        internal UnitHashIndex(Span<Meta> bucket, BucketVersion version)
        {
            this.bucket = bucket;
            this.version = version;
            this.count = 0;
            this.Clear();
        }

       
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        public void Insert(InsertHint hint, int insertIndexItem)
        {

            if (!hint.IsValid)
#if DEBUG
                throw new ArgumentNullException(nameof(hint));
#else
                return;
#endif
            this.count++;

            ref var inserted = ref this.bucket.Insert(
                this.version,
                hint.Index,
                hint.metaData
            );
            inserted = new (insertIndexItem, hint.metaData);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            this.count = 0;
            this.version = this.version.IncrementBucket(out var isOverflow);
            if (isOverflow)
                this.bucket.Clear();
        }

        public void Refresh<TKey>(ReadOnlySpan<TKey> keys)
            where TKey : notnull, IEquatable<TKey>
        {
            this.Clear();
            var version = this.version;
            var bucketVersion = version.Bucket;
            var bucket = this.bucket;
            var count = this.count;
            var hash = 0;
            ref var inserted = ref Unsafe.NullRef<Meta>();
            for (var i = 0; i < keys.Length; ++i)
            { 
                hash = keys[i].GetHashCode();
                inserted = ref this.bucket.Insert(
                    version, 
                    bucket.GetBucketIndex(hash), 
                    Meta.Data.CreateEntry(hash, bucketVersion)
                );
                inserted = inserted.UpdateKeyIndex(i);
            }
        }
    }

}
