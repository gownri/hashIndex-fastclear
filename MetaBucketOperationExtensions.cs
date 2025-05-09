using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndexers
{
    internal static class MetaBucketOperationExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta Insert(this Span<Meta> bucket, BucketVersion version, int insertStartIndex, Meta.Data insertKey)
        {
            ref var container = ref bucket[insertStartIndex];
#if DEBUG
            if ((uint)insertStartIndex >= (uint)bucket.Length)
                throw new ArgumentOutOfRangeException(nameof(insertStartIndex));
#endif
            long bucketVersion = (uint)version.Bucket << Meta.Data.VersionOffset;
            if (((long)container.RawData - (long)bucketVersion) >= 0)
                ShiftInsert(bucket, bucketVersion, insertStartIndex, container);
            container = Meta.Create(insertKey);
            return ref container;

            static void ShiftInsert(Span<Meta> bucket, long bucketVersion, int insertStartIndex, Meta insertItem)
            {
                Meta.Data current;
                JumpType jumpType;
                int jump;

                ref var insertPoint = ref Unsafe.NullRef<Meta>();
                int limit;
                Meta swapTemp;
                do
                {
                    current = insertItem.MashedVDH;
                    limit = Meta.Data.MaxCountableDistance - current.Distance;
                    jumpType = current.GetJumpType();
                    jump = (int)jumpType;
#if DEBUG
                    for (var safe = 0; safe < bucket.Length; ++safe)
#else
                    while (true)
#endif
                    {
                        insertPoint = ref bucket.GetBucket(insertStartIndex + jump, out insertStartIndex);
                        if (limit > 0)
                            current = current.AddJump(jumpType);
                        if (((long)insertPoint.RawData - (long)bucketVersion) < 0) //non use mask
                        {
                            insertPoint = insertItem.Update(current); //new(insertItem.KeyIndex, current);
                            return;
                        }
                        if (insertPoint.RawData < current.RawData)
                        {
                            swapTemp = insertPoint;
                            insertPoint = insertItem.Update(current);
                            insertItem = swapTemp;
                            break;
                        }

                    }
                } while (true);

                //unreachable
#if DEBUG
                throw new NotImplementedException("this line is unreachable");
#endif
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetBucketIndex(this Span<Meta> bucket, int hash)
            => hash & (bucket.Length - 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta GetBucket(this Span<Meta> bucket, int hash)
            => ref Unsafe.Add( ref MemoryMarshal.GetReference(bucket), hash & (bucket.Length - 1) );
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta GetBucket(this Span<Meta> bucket, int hash, scoped out int index)
            => ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), index = hash & (bucket.Length - 1) ); 
            //ref bucket[index = hash & (bucket.Length - 1)];

        internal static ref Meta EntryOrLess(this Span<Meta> bucket, int start, Meta.Data entry, JumpType jumpType,
            scoped out int index, scoped out Meta.Data keyOfSlot)
        {
#if DEBUG
            if ((uint)start >= (uint)bucket.Length)
                throw new ArgumentOutOfRangeException(nameof(start));
#endif
            var jump = (int)jumpType;
            var distanceLimit = Math.Min(Meta.Data.MaxCountableDistance - entry.Distance, bucket.Length);
            var span = Span<Meta>.Empty;
            int pos;
#if !DEBUG
            ref var current = ref Unsafe.NullRef<Meta>();
#endif
            pos = start;
            while (distanceLimit > 0)
            {
#if DEBUG
                ref var
#endif
                current = ref bucket.GetBucket(pos, out pos);
                if (current.RawData > entry.RawData)
                {
                    entry = entry.AddJump(jumpType);
                    pos += jump;
                    distanceLimit -= jump;
                    continue;
                }
                index = pos;
                keyOfSlot = entry;
                return ref current;
            }

            return ref ProbeOverWork(bucket, out index, out keyOfSlot);
            [MethodImpl(MethodImplOptions.NoInlining)]
            ref Meta ProbeOverWork(Span<Meta> bucket, scoped out int index, scoped out Meta.Data keyOfSlot)
            {
                pos = start;

#if DEBUG
                for (var safe = 0; safe < bucket.Length; ++safe)
#else
                while(true)
#endif
                {
                    ref var current = ref bucket[pos];
                    if (current.RawData > entry.RawData)
                    {
                        pos = bucket.GetBucketIndex(pos + jump);
                        entry = entry.AddJump(jumpType);
                        continue;
                    }
                    index = pos;
                    keyOfSlot = entry;
                    return ref current;
                }
                //unreachable
#if DEBUG
                throw new NotImplementedException("this line is unreachable");
#endif
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta FindOrLess<TKey>(
            this Span<Meta> bucket, ReadOnlySpan<TKey> keys,
            int pos, Meta.Data entry, TKey key, JumpType jumpType,
            scoped out bool exist, scoped out int index, scoped out Meta.Data keyOfSlot)
        where TKey : notnull, IEquatable<TKey>
        {
#if DEBUG
            if ((uint)start >= (uint)bucket.Length)
                throw new ArgumentOutOfRangeException(nameof(start));
#endif
            var jump = (int)jumpType;
            var distanceLimit = Math.Min(Meta.Data.MaxCountableDistance - entry.Distance, bucket.Length);
            var existL = false;
#if !DEBUG
            ref var current = ref Unsafe.NullRef<Meta>();
#endif
            while (distanceLimit > 0)
            {
#if DEBUG
                ref var
#endif
                    current = ref bucket.GetBucket(pos, out pos);
                if (((existL = current.RawData == entry.RawData)
                        && !keys[current.KeyIndex].Equals(key))
                    || current.RawData > entry.RawData)
                {
                    entry = entry.AddJump(jumpType);
                    pos += jump;
                    distanceLimit -= jump;
                    continue;
                }
                exist = existL;
                index = pos;
                keyOfSlot = entry;
                return ref current;
            }

            return ref ProbeOverWork(bucket, keys,
                pos, entry, key, jumpType, jump,
                out exist, out index, out keyOfSlot);
            [MethodImpl(MethodImplOptions.NoInlining)]
            static ref Meta ProbeOverWork(Span<Meta> span, ReadOnlySpan<TKey> keys, 
                int pos, Meta.Data entry, TKey key, JumpType jumpType, int jump,
                out bool exist, out int index, out Meta.Data keyOfSlot)
            {
#if DEBUG
                for (var safe = 0; safe < span.Length; ++safe)
#else
                ref var current = ref Unsafe.NullRef<Meta>();
                while (true)
#endif
                {
#if DEBUG
                    ref var
#endif
                        current = ref span.GetBucket(pos, out pos);
                    if (((exist = current.RawData == entry.RawData)
                            && !keys[current.KeyIndex].Equals(key))
                        || current.RawData > entry.RawData)
                    {
                        pos += jump;
                        continue;
                    }
                    index = pos;
                    keyOfSlot = entry;
                    return ref current;
                }
                //unreachable
#if DEBUG
                throw new NotImplementedException("this line is unreachable");
#endif
            }
        }

    }
}
