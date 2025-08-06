#nullable enable
using HashIndexes.InternalModules;
using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace IndexFriendlyCollections;


public class HashIndex<TKey> : IDisposable
    where TKey : notnull, IEquatable<TKey>
{
    private const int keysCapacityOffset = 1;
    public void BucketDebugDisplay()
    {
        var bucket = this.hashBucket.AsSpan();
        foreach(var v in bucket)
        {
            Console.WriteLine(v);
        }
    }
    private BucketVersion version;
    private Meta[] hashBucket;
    private TKey[] keys;

    private int count = 0;
    //private Meta.Data maxCollision = Meta.Data.Initial;

    //[Obsolete]
    //public int MaxCollisions => this.maxCollision.Distance;
    public int BucketSize => this.hashBucket.Length;
    public int Capacity => this.keys.Length - keysCapacityOffset;
    public int Count => this.count;
    public ReadOnlySpan<TKey> Keys => this.keys.AsSpan(0, this.count);
    public uint VersionToken => this.version.RawValue;
    public HashIndex(int capacity, bool useArrayPool = false)
    {
        this.version = BucketVersion.Create();
        this.hashBucket = Array.Empty<Meta>(); 
        this.keys = Array.Empty<TKey>();
        this.Expand(Helper.GetBucketSize(capacity), false, useArrayPool);
#if DEBUG
        this.hashBucket.AsSpan().Fill(new(unchecked((int)0xABADBEEF), default));
#endif
    }

    public void Expand(int newCapacity, bool forceRehash = false, bool useArrayPool = false)
    {
        //var keys = this.keys.AsSpan(..this.count);
        //if (newCapacity > keys.Length)
        //{
        //    var newKeys = useArrayPool 
        //        ? ArrayPool<TKey>.Shared.Rent(newCapacity) 
        //        : new TKey[Helper.GetNextPowerOfTwo(newCapacity)];
        //    keys.CopyTo(newKeys);
        //    ArrayPool<TKey>.Shared.Return(this.keys, RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
        //    this.keys = newKeys;
        //    keys = newKeys.AsSpan(0, keys.Length);
        //}

        //newCapacity = Helper.GetBucketSizeOfKeyCapacity(newCapacity);
        //if(newCapacity > this.hashBucket.Length)
        //{
        //    BucketPool.Return(this.hashBucket, this.version);
        //    this.hashBucket = BucketPool.Rent(newCapacity, out this.version);
        //    forceRehash = true;
        //}

        //if (forceRehash)
        //{
        //    this.VersionIncrement();
        //    this.hashBucket.AsSpan().Rehash(keys, this.version.Bucket);
        //}
        this.Expand(
            Helper.RoundUpToPoolingSize(newCapacity), 
            Helper.RoundUpToPoolingSize(
                Helper.GetBucketRoom(newCapacity)
            ),
            forceRehash, 
            useArrayPool);
    }

    private void Expand(int newKeysCapacity, int newBucketCapacity, bool forceRehash = false, bool useArrayPool = false)
    {
        if (newKeysCapacity > this.keys.Length)
        {
            var newKeys = useArrayPool
                ? ArrayPool<TKey>.Shared.Rent(newKeysCapacity)
                : new TKey[newKeysCapacity];
            Array.Copy(this.keys, newKeys, this.count);
            ArrayPool<TKey>.Shared.Return(this.keys, RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
            this.keys = newKeys;
        }

        if (newBucketCapacity > this.hashBucket.Length)
        {
            BucketPool.Return(this.hashBucket, this.version);
            this.hashBucket = BucketPool.Rent(newBucketCapacity, out var bucketVersion);
            this.version = this.version.ReuseBucket(bucketVersion);
            forceRehash = true;
        }

        if (forceRehash)
        {
            this.VersionIncrement();
            var count = this.count;
            if(count > 0)
                this.hashBucket.AsSpan().Rehash(this.keys.AsSpan(..count), this.version.Bucket);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear(bool ClearAllKeys)
    {
        this.Clear();
        if (ClearAllKeys)
            Array.Clear(this.keys, 0, this.keys.Length);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear()
    {
        this.count = 0;
        this.VersionIncrement();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void VersionIncrement()
    {
        this.version = this.version.IncrementBucket(out var isOverflow);
        if (isOverflow)
            Array.Clear(this.hashBucket, 0, this.hashBucket.Length);
    }
    public bool TryGetIndex(TKey key, [MaybeNullWhen(false)] scoped out int index)
    {
        const int invalidIndex = -1;
        var bucket = this.hashBucket.AsSpan();
        var keys = this.keys.AsSpan(..this.count);

        var hashIndex = key.GetHashCode();
        var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
        var inBucket = bucket.GetBucket(hashIndex, out hashIndex); 
        var indexL = inBucket.KeyIndex;

        if (inBucket.RawData == entryKey.RawData
            && (uint)indexL < (uint)keys.Length
                && keys[indexL].Equals(key))
        {
            index = indexL;
            return true;
        }
        else if (inBucket.RawData < entryKey.RawData)
        {
            index = invalidIndex;
            return false;
        }
        var jump = entryKey.GetJumpType();

        index = bucket.FindOrLess(
            keys,
            (
                hashIndex + (int)jump,
                entryKey.AddJump(jump),
                jump,
                key
            ),
            out var outs
        ).KeyIndex;
        return outs.exist;
    }

    //[Obsolete]
    //public int ExpandableGetIndex(TKey key, out bool exist, byte tolerateCollisions)
    //    => this.GetIndex(key, out exist);
    //[Obsolete]
    //public int ExpandableGetIndex(TKey key, out bool exist, byte tolerateCollisions)
    //{
    //    var version = this.version.Bucket;
    //    var bucket = this.hashBucket.AsSpan();
    //    var hashIndex = key.GetHashCode();
    //    var entryKey = Meta.Data.CreateEntry(hashIndex, version);
    //    var keys = this.keys.AsSpan();
    //    var entry = bucket.GetBucket(hashIndex, out hashIndex);
    //    if (exist = entry.RawData == entryKey.RawData
    //        && keys[entry.KeyIndex].Equals(key))
    //        return entry.KeyIndex;
    //    else if (entry.RawData < entryKey.RawData)
    //    {
    //        exist = false;
    //        return this.Setup(hashIndex, entryKey, key);
    //    }

    //    entry = bucket.FindOrLess(
    //        keys,
    //        (
    //            hashIndex,
    //            entryKey,
    //            entryKey.GetJumpType(),
    //            key
    //        ),
    //        out var outs
    //    );

    //    //checking
    //    if (this.maxCollision.RawData < outs.metaDataOfSlot.RawData)
    //        this.maxCollision = outs.metaDataOfSlot;
    //    if (this.maxCollision.Distance > tolerateCollisions)
    //        this.Expand(keys.Length * 2, true);

    //    exist = outs.exist;
    //    if (exist)
    //        return entry.KeyIndex;
    //    keys = this.keys.AsSpan();
    //    if(this.count + 1 >= keys.Length)
    //        this.Expand(keys.Length * 2);
    //    return this.Setup(outs.indexOfSlot, outs.metaDataOfSlot, key);
    //}
    public int GetIndex(TKey key, out bool exist)
    {
        var bucket = this.hashBucket.AsSpan();
        var keys = this.keys.AsSpan(..this.count);

        var hashIndex = key.GetHashCode();
        var entry = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
        var inBucket = bucket.GetBucket(hashIndex, out hashIndex);

        var indexL = inBucket.KeyIndex;
        if (inBucket.RawData == entry.RawData
            && (uint)indexL < (uint)keys.Length
                && keys[indexL].Equals(key))
        {
            exist = true;
            return indexL;
        }
        else if (inBucket.RawData < entry.RawData)
        {
            exist = false;
            return this.Setup(hashIndex, entry, key);
        }

        var jump = entry.GetJumpType();
        inBucket = bucket.FindOrLess(
            keys,
            (
                hashIndex + (int)jump,
                entry.AddJump(jump),
                jump,
                key
            ),
            out var outs
        );

        if (outs.exist)
        {
            exist = true;
            return inBucket.KeyIndex;
        }

        exist = false;
        return this.Setup(outs.indexOfSlot, outs.metaDataOfSlot, key);
    }

    [MethodImpl (MethodImplOptions.AggressiveInlining)]
    private int Setup(int insertStartIndex, Meta.Data setData, TKey setKey)
    {
        var keys = this.keys;
        var keyIndex = this.count++;
        keys[keyIndex] = setKey;
        var meta = new Meta(keyIndex, setData);
        this.hashBucket.AsSpan().Insert(insertStartIndex, meta, this.version.Bucket);

        if((keyIndex + keysCapacityOffset) >= keys.Length)
            this.Expand(keys.Length * 2);
        //if(setData.Distance > Helper.CollisionTolerance)
        //{
        //    this.Expand(keys.Length, this.hashBucket.Length * 2);
        //}
        return keyIndex;
    }

    public void Dispose()
    {
        this.count = -1;
        this.version = default;
        if (this.hashBucket is not null)
        {
            BucketPool.Return(this.hashBucket, this.version);
            this.hashBucket = null!;
        }

        if (this.keys is not null)
        {
            ArrayPool<TKey>.Shared.Return(this.keys, RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
            this.keys = Array.Empty<TKey>();
        }
    }
}
