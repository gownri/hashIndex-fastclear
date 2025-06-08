#nullable enable
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
namespace HashIndexes
{


    public class HashIndex<TKey>
        where TKey : notnull, IEquatable<TKey>
    {
        
        internal void DebugDisplay()
        {
            for (var i = 0; i < this.hashBucket.Length; ++i)
            {
                ref var meta = ref this.hashBucket[i];
                Console.WriteLine($" index = {i,4} : key = {(meta.KeyIndex < 0 ? "null" : this.keys[meta.KeyIndex]),12} [{meta}]");
            }
        }
        private BucketVersion version;
        private Meta[] hashBucket;
        private TKey[] keys;

        private int count = 0;
        private Meta.Data maxCollision = Meta.Data.Initial;

        public int MaxCollisions => this.maxCollision.Distance;
        public int BucketSize => this.hashBucket.Length;
        public int Capacity => Math.Min(this.keys.Length, this.hashBucket.Length);
        public int Count => this.count;
        [Obsolete]
        public bool IsAddable => 
            this.count < this.keys.Length 
            && this.count < (this.hashBucket.Length - Helper.BucketSentinel);
        public ReadOnlySpan<TKey> Keys => this.keys.AsSpan(0, this.count);
        public uint VersionToken => this.version.RawValue;
        public HashIndex(int capacity)
        {
            capacity = capacity >= 0 ? capacity + 1 : 1;
            this.version = BucketVersion.Create();
            int bucketSize = Helper.GetNextPowerOfTwo(Helper.BucketRoom(capacity));

            this.hashBucket = new Meta[bucketSize];
#if DEBUG
            this.hashBucket.AsSpan().Fill(new(unchecked((int)0xABADBEEF), default));
#endif
            this.keys = new TKey[capacity];
        }

        public void Expand(int newCapacity, bool forceRehash = false)
        {
            if (newCapacity > this.keys.Length)
            {
                var newKeys = new TKey[newCapacity];
                Array.Copy(this.keys, newKeys, this.count);
                this.keys = newKeys;
            }

            newCapacity = Helper.BucketRoom(newCapacity);
            if(newCapacity > this.hashBucket.Length)
            {
                var newBucket = new Meta[
                    Helper.GetNextPowerOfTwo(newCapacity)
                ];
                this.hashBucket = newBucket;
                this.version = BucketVersion.Create();
                forceRehash = true;
            }

            if (forceRehash)
            {
                var keys = this.keys.AsSpan(0, this.count);
                this.Clear();
                this.hashBucket.AsSpan().Rehash(keys, this.version.Bucket);
            }
        }
        private void BucketExpand()
        {
            var keysRetention = this.keys.AsSpan(0, this.count);
            this.Clear();
            this.hashBucket = new Meta[this.hashBucket.Length << 1];
            this.hashBucket.AsSpan().Rehash(keysRetention, this.version.Bucket);
        }

        private void KeysExpand()
        {
            var newKeys = new TKey[this.keys.Length << 1];
            Array.Copy(this.keys, newKeys, this.count);
            this.keys = newKeys;
        }

        internal void BucketCheck()
        {
            if (this.count >= this.keys.Length)
                this.KeysExpand();
            if (this.count >= Helper.BucketRoom(this.hashBucket.Length) )
                this.BucketExpand();
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
            this.version = this.version.IncrementBucket(out var isOverflow);
            this.count = 0;
            if (isOverflow)
            {
                Array.Clear(this.hashBucket, 0, this.hashBucket.Length);
                this.maxCollision = Meta.Data.Initial;
            }
        }

        public bool TryGetIndex(TKey key,[MaybeNullWhen(false)] scoped out int index)
        {
            var hashIndex = key.GetHashCode();
            var bucket = this.hashBucket.AsSpan();
            var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
            var meta = bucket.GetBucket(hashIndex, out hashIndex); 
            if (meta.RawData == entryKey.RawData
                && this.keys[meta.KeyIndex].Equals(key))
            {
                index = meta.KeyIndex;
                return true;
            }
            var jump = entryKey.GetJumpType();
            var jumpLen = (int)jump;
            meta = bucket.GetBucket(hashIndex + jumpLen, out hashIndex);
            entryKey = entryKey.AddJump(jump);
            if (meta.RawData == entryKey.RawData
                && this.keys[meta.KeyIndex].Equals(key))
            {
                index = meta.KeyIndex;
                return true;
            }

            index = bucket.FindOrLess(
                        this.keys,
                        (
                            bucket.GetBucketIndex(hashIndex + jumpLen),
                            entryKey.AddJump(jump),
                            jump,
                            key
                        ),
                        out var outs
                    ).KeyIndex;
            return outs.exist;
        }

        public int ExpandableGetIndex(TKey key, out bool exist, byte tolerateCollisions)
        {
            //checking
            this.BucketCheck();

            var version = this.version.Bucket;
            var bucket = this.hashBucket.AsSpan();
            var hashIndex = key.GetHashCode();
            var entryKey = Meta.Data.CreateEntry(hashIndex, version);
            var keys = this.keys.AsSpan();
            var entry = bucket.GetBucket(hashIndex, out hashIndex); //hashIndex = this.GetBucketIndex(hashIndex);
            if (exist = entry.RawData == entryKey.RawData
                && keys[entry.KeyIndex].Equals(key))
                return entry.KeyIndex;
            else if (entry.RawData < entryKey.RawData)
            {
                exist = false;
                return this.Setup(hashIndex, entryKey, key);
            }

            entry = bucket.FindOrLess(
                keys,
                (
                    hashIndex,
                    entryKey,
                    entryKey.GetJumpType(),
                    key
                ),
                out var outs
            );

            //checking
            if (this.maxCollision.RawData < outs.keyOfSlot.RawData)
                this.maxCollision = outs.keyOfSlot;
            if (this.maxCollision.Distance > tolerateCollisions)
                this.BucketExpand();

            exist = outs.exist;
            if (exist)
                return entry.KeyIndex;

            return this.Setup(outs.indexOfBucket, outs.keyOfSlot, key);
        }
        public int GetIndex(TKey key, out bool exist)
        {
            var version = this.version.Bucket;
            var bucket = this.hashBucket.AsSpan();
            var hashIndex = key.GetHashCode();
            var entryKey = Meta.Data.CreateEntry(hashIndex, version);
            var keys = this.keys.AsSpan();
            var entry = bucket.GetBucket(hashIndex, out hashIndex); //hashIndex = this.GetBucketIndex(hashIndex);

            if(exist = entry.RawData == entryKey.RawData
                && keys[entry.KeyIndex].Equals(key))
                    return entry.KeyIndex;
            else if (entry.RawData < entryKey.RawData)
            {
                exist = false;
                return this.Setup(hashIndex, entryKey, key);
            }

            entry = bucket.FindOrLess(
                keys,
                (   
                    hashIndex,
                    entryKey,
                    entryKey.GetJumpType(),
                    key
                ),
                out var outs
            );

            exist = outs.exist;
            if (exist)
                return entry.KeyIndex;

            return this.Setup(outs.indexOfBucket, outs.keyOfSlot, key);
        }

        [MethodImpl (MethodImplOptions.AggressiveInlining)]
        private int Setup(int insertStartIndex, Meta.Data setData, TKey setKey)
        {
            var keyIndex = this.count++;
            var meta = new Meta(keyIndex, setData);
            this.hashBucket.AsSpan().Insert(insertStartIndex, meta, this.version.Bucket);
            this.keys[keyIndex] = setKey;
            return keyIndex;
        }
    }

}
