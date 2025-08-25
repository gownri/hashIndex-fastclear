#define SAFEBUCKET
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndex.InternalModules;

internal static class MetaHashTableOperationExtensions
{
    const int overLengthMultiply = 17;
    
    /// <summary>
    /// get index of initial or insertedIndex of <paramref name="insertingIndex"/>
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <param name="table"></param>
    /// <param name="key"></param>
    /// <param name="version"></param>
    /// <param name="insertingIndex"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int InitialProve<TKey>(
        this Span<Meta> table, TKey key,
        ushort version, int insertingIndex,
        scoped out ProveContext nextProveContext
    )
        where TKey : notnull
    {
        var hash =
            key.GetHashCode();
        ref var initial = 
            ref Unsafe.Add(ref MemoryMarshal.GetReference(table), hash & (table.Length - 1));
        if (initial.MashedVDH.Version != version)
            initial = new Meta(insertingIndex, Meta.Data.CreateEntry(hash, version));
        nextProveContext = new(hash, true);
        return initial.KeyIndex;
    }
    /// <summary>
    /// get index of initial or -1
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <param name="table"></param>
    /// <param name="key"></param>
    /// <param name="version"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int InitialProve<TKey>(
        this Span<Meta> table, TKey key,
        ushort version, out ProveContext nextProveContext
    )
        where TKey : notnull
    {
        const int invalidIndex = -1;
        var hash =
            key.GetHashCode();
        var initial = table.GetBucket(hash);
        nextProveContext = new(hash, true);
        return initial.MashedVDH.Version == version
            ? initial.KeyIndex
            : invalidIndex;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetBucketIndex(this scoped Span<Meta> table, int hash)
        => hash & (table.Length - 1);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> table, int hash)
        =>
#if SAFEBUCKET
            ref table[hash & (table.Length - 1)];
#else
        ref Unsafe.Add(ref MemoryMarshal.GetReference(table), hash & (table.Length - 1));
#endif
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> table, int hash, scoped out int roundedIndex)
    {
        var index = hash & (table.Length - 1);
        roundedIndex = index;
        return
#if SAFEBUCKET
            ref table[index];
#else
            ref Unsafe.Add(ref MemoryMarshal.GetReference(table), index);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetBucketMask(this Span<Meta> table)
    {
        var length = table.Length;
        if((length & (-length)) == length
            && length != 0)
            return length - 1;
        else
            return Throw();
        int Throw()
            => throw new ArgumentException($"{nameof(table)}.{nameof(table.Length)}:{length} is not power of two or zero");
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> table, int hash, int cachedMask)
        =>
#if SAFEBUCKET
            ref table[hash&cachedMask];
#else
        ref Unsafe.Add(ref MemoryMarshal.GetReference(table), hash & cachedMask);
#endif
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ref Meta GetBucket(this Span<Meta> table, int hash, int cachedMask, scoped out int roundedIndex)
    {
        var index = hash & cachedMask;
        roundedIndex = index;
        return
#if SAFEBUCKET
            ref table[index];
#else
            ref Unsafe.Add(ref MemoryMarshal.GetReference(table), index);
#endif

    }

    internal static int FindOrInserted<TKey>(
        this Span<Meta> table, scoped ReadOnlySpan<TKey> keys,
        TKey key, ProveContext context, ushort version,
        int insertingIndex, bool isInserting,
        scoped out bool exist,
        scoped out int collisionCount
    )
    where TKey : notnull, IEquatable<TKey>
    {

        var pos = context.Hash;
        ref var tableRef = ref MemoryMarshal.GetReference(table);
        var entry = Meta.Data.CreateEntry(pos, version);
        var jumpType = entry.GetJumpType();
        var jump = (int)jumpType;

        long versionComparer;

        var collisionCountL = 0;

        var maskCache = table.GetBucketMask();
        ref var current =
#if SAFEBUCKET
            ref table.GetBucket(pos, maskCache, out pos);
#else
            ref Unsafe.Add(ref tableRef, pos &= maskCache);
#endif
        Meta currentTemp = current;
        int indexOfKeys;

        if (currentTemp.RawData < entry.RawData)
            goto notFoundResult;

        pos +=
            HashCode.Combine(entry.RawData);
        //entry.Hash;


        if (currentTemp.RawData == entry.RawData)
        {
            indexOfKeys = currentTemp.KeyIndex;
            if ((uint)indexOfKeys < (uint)keys.Length
                && keys[indexOfKeys].Equals(key))
            {
                collisionCount = collisionCountL;
                exist = true;
                return indexOfKeys;
            }
        }

        versionComparer = ((long)entry.Version + 1) << Meta.Data.VersionOffset;

#if DEBUG
        int versionEquality = entry.Version;
        for (var safe = 0; safe < table.Length; ++safe)
#else
        while (true)
#endif
        {
            collisionCountL++;
            entry = entry.AddJump(jumpType);
            current =
#if SAFEBUCKET
                ref table.GetBucket(pos + jump, maskCache, out pos);
#else
                ref Unsafe.Add(ref tableRef, pos = (pos + jump) & maskCache);
#endif
            currentTemp = current;

            if (currentTemp.RawData > entry.RawData)
                continue;
            if (currentTemp.RawData == entry.RawData)
            {
                indexOfKeys = currentTemp.KeyIndex;
                if ((uint)indexOfKeys < (uint)keys.Length
                    && keys[indexOfKeys].Equals(key))
                {
                    collisionCount = collisionCountL;
                    exist = true;
                    return indexOfKeys;
                }

                continue;
            }
            else if (entry.RawData >= versionComparer)
            {
                //retesting
                entry = entry.SubJump(jumpType);
                pos -= jump;

                jumpType = JumpType.None;
                jump *= overLengthMultiply;
                continue;
            }

#if DEBUG
            if (versionEquality != entry.Version)
                throw new Exception("versionEquality != entry.Version");
            if (collisionCountL > table.Length)
                throw new InvalidOperationException($"Infinite loop detected during insertion, check your hash function or {nameof(table)} size.");
#endif
            break;
        }

    notFoundResult:;

        collisionCount = collisionCountL;
        exist = false;
        if (!isInserting)
            return -1;
#if DEBUG
        if (keys.Length > table.Length)
            throw new InvalidOperationException($"{nameof(table)} may be full");
#endif
        var insertItem = new Meta(insertingIndex, entry);
        versionComparer = ((long)version) << Meta.Data.VersionOffset;

        current = insertItem;

        if ((currentTemp.RawData - versionComparer) < 0)
            return insertingIndex;

        insertItem = currentTemp;

        entry = insertItem.MashedVDH;
        jumpType = entry.GetJumpType();
        jump = (int)jumpType;

        collisionCountL = 0;

        long limit = Meta.Data.MaxCountableDistance - entry.Distance;
        if (limit == 0)
        {
            jump *= overLengthMultiply;
            jumpType = JumpType.None;
        }

        if (limit == Meta.Data.MaxCountableDistance)
            pos +=
                HashCode.Combine(entry.RawData);
        //entry.Hash;


        do
        {
            while (true)
            {
                collisionCountL++;
                entry = entry.AddJump(jumpType);
                current =
#if SAFEBUCKET
                    ref table.GetBucket(pos + jump, maskCache, out pos);
#else
                    ref Unsafe.Add(ref tableRef, pos = (pos + jump) & maskCache);
#endif
                currentTemp = current;

                if (currentTemp.RawData < entry.RawData)
                    if ((currentTemp.RawData - versionComparer) < 0)
                    {
                        current = insertItem.Update(entry);
                        collisionCount = Math.Max(collisionCountL, collisionCount);
                        return insertingIndex;
                    }
                    else
                    {
                        current = insertItem.Update(entry);
                        insertItem = currentTemp;
                        break;
                    }

                limit = unchecked(limit - jump);

                if (limit == 0)
                {
                    jump *= overLengthMultiply;
                    jumpType = JumpType.None;
                }

#if DEBUG
                if (collisionCountL > table.Length)
                    throw new InvalidOperationException("no capacity");
#endif
            }

            collisionCountL = 0;
            entry = insertItem.MashedVDH;
            limit = Meta.Data.MaxCountableDistance - entry.Distance;
            jumpType = entry.GetJumpType();
            jump = (int)jumpType;

            if (limit == 0)
            {
                jump *= overLengthMultiply;
                jumpType = JumpType.None;
            }

            if (limit == Meta.Data.MaxCountableDistance)
                pos +=
                    HashCode.Combine(entry.RawData);
            //entry.Hash;


        } while (true);
    }

    internal static void Rehash<TKey>(
        this Span<Meta> table, ReadOnlySpan<TKey> uniqueKeys,
        ushort version
    )
    where TKey : notnull, IEquatable<TKey>
    {
        bool exist;
        int mask = table.GetBucketMask();
        int keyIndex;
        ProveContext context;
        for (var i = 0; i < uniqueKeys.Length; i++)
        {
            keyIndex = table.InitialProve(uniqueKeys[i], version, i, out context);

            if (keyIndex == i)
                continue;
            else
            {
                table.FindOrInserted(
                    uniqueKeys,
                    uniqueKeys[i],
                    context,
                    version,
                    i,
                    true,
                    out exist,
                    out _
                );
                if (exist)
                    ThrowDuplicateKeys();
            }

            static void ThrowDuplicateKeys() => throw new InvalidOperationException($"{nameof(uniqueKeys)} contains non unique items");

        }
    }

    public static ReadOnlySpan<KeyIndexSearchMeta> AsKeyIndexSearch(this Span<Meta> container)
        => MemoryMarshal.Cast<Meta, KeyIndexSearchMeta>(container);
}
