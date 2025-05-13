using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexes
{
    public static class UnitHashIndexKeyOperationExtensions
    {

        //public void Insert(TKey key, int insertIndexItem)
        //{
        //    var entryEnumerable = this.GetEntries(key);
        //    InsertHint hint = default;
        //    if (entryEnumerable.TryReadInitExist(out var insertHint))
        //        foreach (var context in entryEnumerable)
        //            hint = context.InsertHint;
        //    else
        //        hint = insertHint;
        //    this.Insert(hint, insertIndexItem);
        //}

        //[MethodImpl(MethodImplOptions.NoInlining)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Entry GetEntries<TKey>(this scoped ref UnitHashIndex unit, TKey key)
            where TKey : notnull
        {
            var hashIndex = key.GetHashCode();
            var entry = Meta.Data.CreateEntry(hashIndex, unit.Version.Bucket);
            var entryRef = unit.bucket.GetBucket(hashIndex, out hashIndex);
            if (entryRef.RawData <= entry.RawData)
                return new(default, hashIndex, entry);

            _ = unit.bucket.EntryOrLess(
                (
                    hashIndex,
                    entry,
                    entry.GetJumpType()
                ),
                out hashIndex,
                out entry
            );

            return new(unit.bucket, hashIndex, entry);
        }

        //public static Index GetIndex<TKey>(this UnitHashIndex unit, ReadOnlySpan<TKey> keys, TKey key, out bool exist)
        //{
        //    var hashIndex = key.GetHashCode();
        //    var entry = Meta.Data.CreateEntry(hashIndex, unit.Version.Bucket);
        //}


        public static bool TryGetIndex<TKey>(this UnitHashIndex unit, ReadOnlySpan<TKey> keys, TKey key, out Index index)
            where TKey : notnull, IEquatable<TKey>
        {
            var hashIndex = key.GetHashCode();
            var entry = Meta.Data.CreateEntry(hashIndex, unit.Version.Bucket);
            var entryRef = unit.bucket.GetBucket(hashIndex, out hashIndex);
            if (entryRef.RawData <= entry.RawData)
            {
                index = default;
                return false;
            }
            _ = unit.bucket.FindOrLess(
                keys,
                (
                    hashIndex,
                    entry,
                    entry.GetJumpType(),
                    key
                ),
                out var outs
            );
            index = outs.indexOfBucket;
            return outs.exist;
        }
    }
}
