using HashIndex.InternalModules;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace IndexFriendlyCollections;


public class HashIndex<TKey> : IDisposable
    where TKey : notnull, IEquatable<TKey>
{
    public const int SupportedTableSize = 0x4000_0000;
    public const int SupportedMaxCapacity = SupportedTableSize - 1;
    public System.Collections.Generic.IEnumerable<string> HashTableDebugDisplay()
    {
        var table = this.table;
        foreach (var v in table)
            yield return $"key:{((uint)v.KeyIndex < (uint)this.keys.Length
                    ? this.keys[v.KeyIndex].ToString()
                    : "null"
                ),12} {{{v}}}";

    }

#if DEBUG
    public int MaxCollisions { get; private set; }
#endif
    private HashTableVersion version;
    private Meta[] table;
    private TKey[] keys;
    internal ref TKey[] KeysRef
        => ref this.keys;
    private int count = 0;
    public int HashTableSize => this.table.Length;
    public int Capacity => this.keys.Length;
    public int Count => this.count;
    public ReadOnlySpan<TKey> Keys => this.keys.AsSpan(0, this.count);
    public uint VersionToken => this.version.RawValue;
    public HashIndex(int capacity = Helper.KeysFloor, int hashTableSize = -1, bool usePoolingTable = true)
    {
        this.version = HashTableVersion.Create();
        this.table = Array.Empty<Meta>(); 
        this.keys = Array.Empty<TKey>();
        hashTableSize = hashTableSize > capacity
            ? hashTableSize
            : capacity * 3 | Helper.TableFloor;
        this.Expand(capacity, hashTableSize, false, usePoolingTable);
#if DEBUG
        this.table.AsSpan().Fill(new(unchecked((int)0xABADBEEF), default));
#endif
    }

    public void Expand(int newKeysCapacity, bool forceRehash = false, bool usePoolingTable = true)
    {
        var table = newKeysCapacity * 3;
        this.Expand(
            newKeysCapacity,
            (uint)table >= (uint)SupportedTableSize ? SupportedTableSize : table,
            forceRehash,
            usePoolingTable
        );
    }

    public void Expand(int newKeysCapacity, int newTableSize, bool forceRehash = false, bool usePoolingTable = true)
    {
        var keys = this.keys;
        var count = this.count;
        var table = this.table;
        if ((uint)newKeysCapacity > (uint)SupportedMaxCapacity)
            ThrowKeysCapacity();
        if (newKeysCapacity > keys.Length)
        {
            var newKeys = new TKey[newKeysCapacity];
            Array.Copy(keys, newKeys, count);
            this.keys = newKeys;
            keys = newKeys;
        }
        newKeysCapacity = keys.Length;
        
        if (newTableSize <= newKeysCapacity)
            newTableSize = newKeysCapacity + 1;
        if (newTableSize > table.Length)
        {
            if ((uint)newTableSize > (uint)SupportedTableSize)
                newTableSize = SupportedTableSize;
            HashTablePool.Return(table, this.version);
            if(usePoolingTable)
                table = HashTablePool.Rent(newTableSize, out this.version);
            else
            {
                table = new Meta[Helper.GetTableSize(newTableSize)];
                this.version = HashTableVersion.Create();
            }
            forceRehash = count > 0;
            this.table = table.Length != 0 
                ? table
                : new Meta[Helper.GetTableSize(newTableSize)];
        }

        if (forceRehash)
            this.Rehash();

        static void ThrowKeysCapacity()
            => throw new ArgumentOutOfRangeException($"arg:{nameof(newKeysCapacity)} was outside the range of 0 to {nameof(SupportedMaxCapacity)}:{SupportedMaxCapacity}.");
        //static void ThrowTableSize()
        //    => throw new ArgumentOutOfRangeException($"arg:{nameof(newTableSize)} was outside the range of {nameof(SupportedTableSize)}:{SupportedTableSize}" +
        //    $"or less than of {nameof(newKeysCapacity)}.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Rehash()
    {
        var keys = new ReadOnlySpan<TKey>(this.keys, 0, this.count);
        this.Clear();
        if (keys.Length > 0)
            this.table.AsSpan().Rehash(
                keys,
                this.version.HashTable
            );
        this.count = keys.Length;
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
        this.version = this.version.IncrementTable(out var isOverflow);
        if (isOverflow)
            Array.Clear(this.table, 0, this.table.Length);
#if DEBUG
        this.MaxCollisions = 0;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetIndex(TKey key,[MaybeNullWhen(false)] out int index)
    {
        var table = this.table.AsSpan();
        var keys = this.keys.AsSpan(..this.count);
        var version = this.version.HashTable;

        var indexL = table.InitialProve(key, version, out var context);
        index = indexL;
        if ((uint)indexL < (uint)keys.Length)
            if(keys[indexL].Equals(key))
                return true;
        else
            return false;

        index = table.FindOrInserted(
            keys,
            key,
            context,
            version,
            -1,
            false,
            out var exist,
            out _
        );

        return exist;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetIndex(TKey key, out bool exist)
    {
        var table = this.table.AsSpan();
        var next = this.count;
        var keys = this.keys.AsSpan(0, next);
        var version = this.version.HashTable;

        var index = table.InitialProve(key, version, next, out var context);
        if (exist = ((uint)index < (uint)keys.Length
            && keys[index].Equals(key)))
            return index;
        else if (index == next)
            return Setup(key, 0);

        index = table.FindOrInserted(
            keys,
            key,
            context,
            version,
            next,
            true,
            out exist,
            out var collisionCount
        );

        if (exist)
            return index;
        else
            return Setup(key, collisionCount);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Setup(TKey setKey, int collisionCount)
        {
    #if DEBUG
            MaxCollisions = Math.Max(collisionCount, MaxCollisions);
    #endif
            var keys = this.keys;
            var keyIndex = this.count;
            if ((uint)keyIndex >= (uint)keys.Length
                || collisionCount > Helper.CollisionTolerance)
                ExpandAdd(keyIndex, setKey, collisionCount);
            else 
                keys[keyIndex] = setKey;
            this.count = keyIndex + 1;
            return keyIndex;

            [MethodImpl(MethodImplOptions.NoInlining)]
            void ExpandAdd(int index, TKey setKey, int collisionCount)
            {
                var keys = this.keys;
                int newSize;
                if((uint)index < (uint)keys.Length)
                    keys[index] = setKey;
                else
                {
                    newSize = 
                        keys.Length > 0
                            ? keys.Length * 2
                            : Helper.KeysFloor;
                    if ((uint)newSize >= (uint)SupportedMaxCapacity)
                        newSize = SupportedMaxCapacity;
                    var newTableSize = newSize * 3;
                    if ((uint)newTableSize >= (uint)SupportedTableSize)
                        newTableSize = SupportedTableSize;

                    this.Expand(
                        newSize,
                        newTableSize
                    );
                    keys = this.keys;
                    keys[index] = setKey;
                }
                newSize = this.table.Length;
                if (collisionCount > Helper.CollisionTolerance
                    && keys.Length * 2 > newSize)
                    this.Expand(0, newSize * 2);
                
            }
        }
    }

    public void Dispose()
    {
        this.count = -1;
        if (this.table is not null)
        {
            HashTablePool.Return(this.table, this.version);
            this.table = null!;
        }

        this.version = default;
        this.keys = Array.Empty<TKey>();

    }
}
