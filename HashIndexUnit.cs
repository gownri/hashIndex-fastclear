using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace HashIndexers
{
    public ref struct HashIndexUnit<TKey>
        where TKey : notnull, IEquatable<TKey>
    {
        private Span<Meta> bucket;
        private int count;
        private BucketVersion version;

        public readonly int Count => this.count;
        public readonly Entry GetEntries(TKey key)
        {

            var hash = key.GetHashCode();
            var entry = Meta.Data.CreateEntry(hash, this.version.Bucket);
            var jumpType = entry.GetJumpType();
            ref var entryRef = ref this.bucket.EntryOrLess(
                hash,
                entry,
                jumpType,
                out var index,
                out entry
            );
            return new Entry(this.bucket, entry, index, (int)jumpType);
        }
        public void Insert(TKey key, int insertIndexItem)
        {
            //    var hash = key.GetHashCode();
            //    var entry = Meta.Data.CreateEntry(hash, this.version.Bucket);
            //    var jumpType = entry.GetJumpType();
            //    this.bucket.EntryOrLess(
            //        this.bucket.GetBucketIndex(hash),
            //        entry,
            //        jumpType,
            //        out var index,
            //        out entry
            //    ); 

            var entryEnumerable = this.GetEntries(key);
            InsertHint hint = default;
            foreach (var context in entryEnumerable)
                hint = context.hint;
            this.Insert(hint, insertIndexItem);
        }

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

        public void Clear()
        {
            this.count = 0;
            this.version = this.version.IncrementBucket(out var isOverflow);
            if (isOverflow)
                this.bucket.Clear();
        }

        public void Refresh(ReadOnlySpan<TKey> keys)
        {
            this.Clear();
            var version = this.version;
            var bucketVersion = version.Bucket;
            var bucket = this.bucket;
            var count = this.count;
            var hash = 0;
            foreach (var key in keys){

                hash = key.GetHashCode();
                this.bucket.Insert(
                    version, 
                    bucket.GetBucketIndex(hash), 
                    Meta.Data.CreateEntry(hash, bucketVersion)
                );
            }
        }
    }

}
