using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndexes
{
    internal static class MetaBucketOperationExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Insert(this Span<Meta> bucket, int insertPoint, Meta insertItem, ushort version)
        {
            ref var initial = ref bucket.GetBucket(insertPoint);
            long bucketVersion = (uint)version << Meta.Data.VersionOffset;
            if (((long)initial.RawData - (long)bucketVersion) < 0)
            {
                initial = insertItem;
                return;
            }
            ShiftInsert(bucket, bucketVersion, insertPoint, insertItem);
            return;

            static void ShiftInsert(Span<Meta> bucket, long bucketVersion, int insertPoint, Meta insertItem)
            {
                Meta.Data current;
                JumpType jumpType;
                int jump;
                int limit;
                ref var target = ref Unsafe.NullRef<Meta>();

                Meta swapTemp;
                do
                {
                    current = insertItem.MashedVDH;
                    jumpType = current.GetJumpType();
                    jump = (int)jumpType;
                    limit = Meta.Data.MaxCountableDistance - current.Distance;
                    while (true)
                    {
                        //only JumpType = {1, 3, 5}
                        if (limit != 0){
                            current = current.AddJump(jumpType);
                            limit -= jump;
                        }
                        target = ref bucket.GetBucket(insertPoint + jump, out insertPoint);
                        if (target.RawData < current.RawData)
                            if ((long)target.RawData - (long)bucketVersion < 0)
                            {
                                target = insertItem.Update(current);
                                return;
                            }
                            else
                            {
                                swapTemp = target;
                                target = insertItem.Update(current);
                                insertItem = swapTemp;
                                break;
                            }
                    }
                } while (true);
            }
        }
        [Obsolete]
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
        internal static int GetBucketIndex(this Span<Meta> bucket, int hashIndex)
            => hashIndex & (bucket.Length - 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta GetBucket(this Span<Meta> bucket, int hashIndex)
            => ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), hashIndex & (bucket.Length - 1) );
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref Meta GetBucket(this Span<Meta> bucket, int hashIndex, scoped out int roundedIndex)
            => ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), roundedIndex = hashIndex & (bucket.Length - 1));

        [Obsolete]
        internal static ref Meta EntryOrLess(this Span<Meta> bucket, 
            scoped in (int start, Meta.Data entry, JumpType jumpType) args,
            scoped out int index, scoped out Meta.Data keyOfSlot)
        {
            var (start, entry, jumpType) = args;
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

        internal static ref Meta FindOrLess<TKey>(
            this Span<Meta> bucket, ReadOnlySpan<TKey> keys,
            scoped in (int start, Meta.Data entry, JumpType jumpType, TKey key) args,
            scoped out (bool exist, int indexOfBucket, Meta.Data keyOfSlot) results
        )
        where TKey : notnull, IEquatable<TKey>
        {
            var (pos, entry, jumpType, key) = args;

            var jump = (int)jumpType;
            var distanceLimit = Math.Min(Meta.Data.MaxCountableDistance - entry.Distance, bucket.Length);
            var existL = false;
#if !DEBUG
            ref var current = ref Unsafe.NullRef<Meta>();
#endif
            //if unrolling as {1, 3, 5} * 17
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
                results = (existL, pos, entry);
                return ref current;
            }

            return ref ProbeOverWork(bucket, keys,
                pos, entry, key, jumpType, jump,
                out results);

            [MethodImpl(MethodImplOptions.NoInlining)]
            static ref Meta ProbeOverWork(Span<Meta> span, ReadOnlySpan<TKey> keys, 
                int pos, Meta.Data entry, TKey key, JumpType jumpType, int jump,
                scoped out (bool exist, int index, Meta.Data keyOfIndex) results)
            {
                //var (pos, entry, key, jumpType, jump) = args;
                var exist = false;
                
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
                    //index = pos;
                    //keyOfSlot = entry;
                    results = (exist, pos, entry);
                    return ref current;
                }
                //unreachable
#if DEBUG
                throw new NotImplementedException("this line is unreachable");
#endif
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Rehash<TKey>(this Span<Meta> bucket, Span<TKey> keys, ushort version)
            where TKey : notnull
        {
            int hashIndex;
            Meta.Data entry;
            for(var i = 0; i < keys.Length; i++)
            {
                hashIndex = keys[i].GetHashCode();
                entry = Meta.Data.CreateEntry(hashIndex, version);
                bucket.Insert(hashIndex, new(i, entry), version);
            }
        }
    }

}
