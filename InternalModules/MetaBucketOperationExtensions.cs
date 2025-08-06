using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndexes.InternalModules;

internal static class MetaBucketOperationExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Insert(this scoped Span<Meta> bucket, int insertPoint, Meta insertItem, ushort version)
    {
        ref var initial = ref bucket.GetBucket(insertPoint);
        long bucketVersion = (uint)version << Meta.Data.VersionOffset;
        if ((initial.RawData - bucketVersion) < 0)
        {
            initial = insertItem;
            return;
        }
        ShiftInsert(bucket, bucketVersion, insertPoint, insertItem);
        return;

        static void ShiftInsert(scoped Span<Meta> bucket, long bucketVersion, int insertPoint, Meta insertItem)
        {
            Meta.Data entry;
            JumpType jumpType;
            int jump;
            int jumpLimit;
            scoped ref var target = ref Unsafe.NullRef<Meta>();
            Meta targetTemp;
            var maskCache = bucket.GetBucketMask();
            //Meta swapTemp;
            do
            {
                entry = insertItem.MashedVDH;
                jumpLimit = Meta.Data.MaxCountableDistance - entry.Distance;
                jumpType = entry.GetJumpType();
#if DEBUG
                if(limit < 0)
                    throw new Exception($"{nameof(limit)}:{limit}"
#endif
                if (jumpLimit == 0)
                    jump = 1;
                else
                    jump = (int)jumpType;

                while (true)
                {
                    //only JumpType = {1, 3, 5}
                    if (jumpLimit != 0){
                        entry = entry.AddJump(jumpType);
                        jumpLimit -= jump;
                    }
                    target = ref bucket.GetBucket(insertPoint + jump, maskCache, out insertPoint);
                    targetTemp = target;
                    if (targetTemp.RawData < entry.RawData)
                        if ((targetTemp.RawData - bucketVersion) < 0)
                        {
                            target = insertItem.Update(entry);
                            return;
                        }
                        else
                        {
                            target = insertItem.Update(entry);
                            insertItem = targetTemp;
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
        if (container.RawData - bucketVersion >= 0)
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
                    if (insertPoint.RawData - bucketVersion < 0) //non use mask
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
    internal static int GetBucketIndex(this scoped Span<Meta> bucket, int hash)
        => hash & bucket.Length - 1;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> bucket, int hash)
        => ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), hash & bucket.Length - 1 );
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> bucket, int hash, scoped out int roundedIndex)
    {
        var index = hash & bucket.Length - 1;
        roundedIndex = index;
        return ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetBucketMask(this Span<Meta> bucket)
        => bucket.Length - 1;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> bucket, int hash, int cachedMask, scoped out int roundedIndex)
    {
        var index = hash & cachedMask;
        roundedIndex = index;
        return ref Unsafe.Add(ref MemoryMarshal.GetReference(bucket), index);
    }

    [Obsolete]
    internal static ref Meta EntryOrLess(this Span<Meta> bucket, 
        scoped in (int start, Meta.Data entry, JumpType jumpType) args,
        scoped out int index, scoped out Meta.Data metaDataOfSlot)
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
            metaDataOfSlot = entry;
            return ref current;
        }

        return ref ProbeOverWork(bucket, out index, out metaDataOfSlot);
        [MethodImpl(MethodImplOptions.NoInlining)]
        ref Meta ProbeOverWork(Span<Meta> bucket, scoped out int index, scoped out Meta.Data metaDataOfSlot)
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
                metaDataOfSlot = entry;
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
        scoped out (bool exist, int indexOfSlot, Meta.Data metaDataOfSlot) results
    )
    where TKey : notnull, IEquatable<TKey>
    {
        var (pos, entry, jumpType, key) = args;

        var jump = (int)jumpType;
        var distanceLimit = Math.Min(Meta.Data.MaxCountableDistance - entry.Distance, bucket.Length);
        var existL = false;
        var maskCache = bucket.GetBucketMask();
#if !DEBUG
        ref var current = ref Unsafe.NullRef<Meta>();
#endif
        Meta currentTemp;
        int indexOfKeys;
        //if unrolling as {1, 3, 5} * 17
        while (distanceLimit > 0)
        {
#if DEBUG
        ref var
#endif
            current = ref bucket.GetBucket(pos, maskCache, out pos);
            currentTemp = current;
            indexOfKeys = currentTemp.KeyIndex;
            if (currentTemp.RawData == entry.RawData
                    && (existL = (uint)indexOfKeys < (uint)keys.Length
                        && keys[indexOfKeys].Equals(key)) //find
                || currentTemp.RawData < entry.RawData) //less
            {
                results = (existL, pos, entry);
                return ref current;
            }

            entry = entry.AddJump(jumpType);
            pos += jump;
            distanceLimit -= jump;
            continue;
        }

        return ref ProbeOverWork(
            bucket, keys,
            pos, entry, key,
            out results
        );

        [MethodImpl(MethodImplOptions.NoInlining)]
        static ref Meta ProbeOverWork(
            Span<Meta> span, ReadOnlySpan<TKey> keys, 
            int pos, Meta.Data entry, TKey key,
            scoped out (bool exist, int index, Meta.Data keyOfIndex) results)
        {
            var existL = false;

            int jump = 1;
            int indexOfKeys;

#if !DEBUG
            ref var current = ref Unsafe.NullRef<Meta>();
#endif
            Meta currentTemp;
            for (var safe = 0; safe < span.Length; ++safe)
            {
#if DEBUG
                ref var
#endif
                current = ref span.GetBucket(pos, out pos);
                currentTemp = current;
                indexOfKeys = currentTemp.KeyIndex;
                if (currentTemp.RawData == entry.RawData
                        && (existL = (uint)indexOfKeys < (uint)keys.Length
                            && keys[indexOfKeys].Equals(key)) //find
                    || currentTemp.RawData < entry.RawData) //less
                {
                    results = (existL, pos, entry);
                    return ref current;
                }
                pos += jump;
                continue;
            }

            throw new IndexOutOfRangeException();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Rehash<TKey>(this Span<Meta> bucket, Span<TKey> uniqueKeys, ushort version)
        where TKey : notnull
    {
        int hashIndex;
        Meta.Data entry;
        for(var i = 0; i < uniqueKeys.Length; i++)
        {
            hashIndex = uniqueKeys[i].GetHashCode();
            entry = Meta.Data.CreateEntry(hashIndex, version);
            bucket.Insert(hashIndex, new(i, entry), version);
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool FirstSearch<TKey>(
        this scoped Span<Meta> bucket,
        scoped Span<TKey> keys,
        TKey key,
        BucketVersion version,
        scoped out (int nextStart, Meta.Data entry, JumpType jumpType, TKey key) nextContext,
        scoped out int existedKeyIndex
    )
        where TKey : notnull, IEquatable<TKey>
    {
        var hash = key.GetHashCode();
        var entry = Meta.Data.CreateEntry(hash, version.Bucket);
        var inBucket = bucket.GetBucket(hash, out var index);

        var keyIndex = inBucket.KeyIndex;
        if (inBucket.RawData == entry.RawData
            && (uint)keyIndex < (uint)keys.Length
                && keys[keyIndex].Equals(key))
        {
            existedKeyIndex = keyIndex;
            nextContext = default;
            return true;
        }

        existedKeyIndex = -1;
        var jumpType = entry.GetJumpType();
        nextContext = (
            index + (int)jumpType,
            entry.AddJump(jumpType),
            jumpType,
            key
        );
        return false;
    }
    /// <summary>
    /// low-level
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <param name="bucket"></param>
    /// <param name="keys"></param>
    /// <param name="key"></param>
    /// <param name="version"></param>
    /// <returns></returns>
    internal static int GetIndexOfKeys<TKey>(
        this scoped Span<Meta> bucket,
        scoped ReadOnlySpan<TKey> keys,
        TKey key,
        BucketVersion version)
    where TKey : notnull, IEquatable<TKey>
    {
        var pos = key.GetHashCode();
        var entry = Meta.Data.CreateEntry(pos, version.Bucket);
        scoped ref var bucketRef = ref MemoryMarshal.GetReference(bucket);
        var maskCache = bucket.Length - 1;
        pos = maskCache & pos;
        var current = Unsafe.Add(ref bucketRef, pos);
        //ref var current = ref bucket.GetBucket(pos, maskCache, out pos);
        var keyIndex = current.KeyIndex;


        if (current.RawData == entry.RawData
            && (uint)keyIndex < (uint)keys.Length
                && keys[keyIndex].Equals(key))
            return keyIndex;
        //else if (entry.RawData > current.RawData)
        //    goto Insert;
        var existL = false;

        var jumpType = entry.GetJumpType();
        var jumpLen = (int)jumpType;

        entry = entry.AddJump(jumpType);
        pos += jumpLen;

        while (true)
        {
            //*17 - 1
            #region unrolling
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else{
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                    || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
            }
            else
            {
                if (existL)
                    return keyIndex;
                else
                    break;
            }
            #endregion
            pos = maskCache & pos;
            current = Unsafe.Add(ref bucketRef, pos);
            keyIndex = current.KeyIndex;

            if (entry.RawData < current.RawData
                 || (entry.RawData == current.RawData
                    && !(existL =
                        (uint)keyIndex < (uint)keys.Length
                        && keys[keyIndex].Equals(key))))
            {
                entry = entry.AddJump(jumpType);
                pos += jumpLen;
                continue;
            }

            if (existL)
                return keyIndex;
            else if(entry.Version != version.Bucket)
            {
                entry = entry.SubJump(jumpType);
                jumpType = JumpType.None;
                continue;
            }

            break;
        }

        //insert:;
        var ret = keys.Length;

        //pos = maskCache & pos;
        ref var currentRef = ref Unsafe.Add(ref bucketRef, pos);
        current = currentRef;

        var swap = new Meta(ret, entry);
        long bucketVersion = (uint)version.Bucket << Meta.Data.VersionOffset;
        
        currentRef = swap;
        if (current.RawData - bucketVersion < 0)
            return ret;

        swap = current;
        entry = swap.MashedVDH;
        jumpType = entry.GetJumpType();
        jumpLen = (int)jumpType;

        while (true)
        {

            entry = entry.AddJump(jumpType);
            pos = maskCache & (pos + jumpLen);
            currentRef = ref Unsafe.Add(ref bucketRef, pos);
            current = currentRef;
            if (entry.RawData > current.RawData)
            {
                if (entry.Version != version.Bucket)// overflowed
                {
                    throw new NotImplementedException();
                }

                currentRef = swap.Update(entry);
                if (current.RawData - bucketVersion < 0)
                    return ret;
                else
                {
                    swap = current;
                    entry = swap.MashedVDH;
                    jumpType = entry.GetJumpType();
                    jumpLen = (int)jumpType;
                    continue;
                }
            }
        }
    }
}
