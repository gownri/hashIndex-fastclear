using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexers
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
        public static Entry GetEntries<TKey>(ref this UnitHashIndex unit, TKey key)
            where TKey : notnull
        {

            var hashIndex = key.GetHashCode();
            var entry = Meta.Data.CreateEntry(hashIndex, unit.Version.Bucket);
            var entryRef = unit.BucketSource.GetBucket(hashIndex, out hashIndex);
            if (entryRef.RawData <= entry.RawData)
                return new(default, ~hashIndex);

            var found = unit.BucketSource.EntryOrLess(
                hashIndex,
                entry,
                entry.GetJumpType(),
                out hashIndex,
                out entry
            );

            return new(unit.BucketSource, hashIndex);
        }

    }
}
