#nullable enable
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
namespace HashIndexers
{


    public class HashIndex<TKey>
        where TKey : notnull, IEquatable<TKey>
    {
        //[Conditional("DEBUG")]
        public void DebugDisplay()
        {
            for (var i = 0; i < this.hashBucket.Length; ++i)
            {
                ref var meta = ref this.hashBucket[i];
                Console.WriteLine($" index = {i, 3} : key = {(meta.KeyIndex < 0 ? "-1": this.keys[meta.KeyIndex]), 12} [ {meta} ]");
            }
        }
        private BucketVersion version;
        private Meta[] hashBucket;
        private TKey[] keys;

        private int count = 0;
        private int maxCollisionDistance = 0;
        public int BufferSize => this.hashBucket.Length;
        public int Count => this.count;
        public bool IsAddable => this.count < (this.hashBucket.Length - Helper.Sentinel);
        public ReadOnlySpan<TKey> Keys => this.keys.AsSpan(0, this.count);
        
        public HashIndex(int initSize)
        {
            initSize++;
            initSize += Helper.Sentinel;
            this.version = BucketVersion.Create();
            int bucketSize = Helper.GetNextPowerOfTwo(initSize);

            this.hashBucket = new Meta[bucketSize];
#if DEBUG
            this.hashBucket.AsSpan().Fill(new(unchecked((int)0xABADBEEF), default));
#endif
            this.keys = new TKey[initSize];
        }

        public void Expand(int newSize, bool forceRehash)
        {
            if (newSize > this.keys.Length)
            {
                var newKeys = new TKey[newSize];
                Array.Copy(this.keys, newKeys, this.count);
                this.keys = newKeys;
            }

            if(newSize > this.hashBucket.Length)
            {
                var newBucket = new Meta[Helper.GetNextPowerOfTwo(newSize)];
                this.hashBucket = newBucket;
                this.version = BucketVersion.Create();
                forceRehash = true;
            }

            if (forceRehash)
            {
                var keys = this.keys.AsSpan(0, this.count);
                this.Clear();
                foreach (var key in keys)
                    _ = this.GetIndex(key, out _);
            }
        }
        private void BucketExpand()
        {
            var keysRetention = this.keys.AsSpan(0, this.count);
            this.Clear();
            this.hashBucket = new Meta[this.hashBucket.Length << 1];

            foreach(var key in keysRetention)
                _ = this.GetIndex(key, out _);
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
            if (this.count*2 >= this.hashBucket.Length 
                || this.maxCollisionDistance > Meta.Data.MaxCountableDistance)
                this.BucketExpand();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(bool isClearKeys)
        {
            this.Clear();
            if (isClearKeys)
                Array.Clear(this.keys, 0, this.keys.Length);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            this.version = this.version.IncrementBucket(out var isOverflow);
            if (isOverflow)
                Array.Clear(this.hashBucket, 0, this.hashBucket.Length);
            this.count = 0;
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetIndex(TKey key,[MaybeNullWhen(false)] scoped out Index index)
        {
            var hashIndex = key.GetHashCode();
            var bucket = this.hashBucket.AsSpan();
            var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
            var meta = bucket.GetBucket(hashIndex, out hashIndex); //ref this.hashBucket[hashIndex];
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
                    bucket.GetBucketIndex(hashIndex + jumpLen),
                    entryKey.AddJump(jump),
                    key,
                    jump,
                    out var exist,
                    out _,
                    out _
                ).KeyIndex;
            return exist;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Index GetIndex(TKey key, out bool exist, bool isExpandable)
        {
            if(isExpandable)
                this.BucketCheck();
            return this.GetIndex(key, out exist);
        }
        public Index GetIndex(TKey key, out bool exist)
        {
#if DEBUG
            if (this.hashBucket.Length <= this.count)
                throw new InvalidOperationException($"{nameof(this.hashBucket)} is full");
#endif
            var bucket = this.hashBucket.AsSpan();
            var hashIndex = key.GetHashCode();
            var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
            ref var entry = ref bucket.GetBucket(hashIndex, out hashIndex); //hashIndex = this.GetBucketIndex(hashIndex);
            if (entry.MashedVDH.Version != this.version.Bucket){
                exist = false;
                return this.Setup(ref entry, entryKey, key);
            }

            entry = ref bucket.FindOrLess(
                this.keys,
                hashIndex,
                entryKey,
                key,
                entryKey.GetJumpType(),
                out exist,
                out var index,
                out var keyOfSlot
            );

            if (exist)
                return entry.KeyIndex;

            return this.Setup(
                ref bucket.Insert(this.version, index, keyOfSlot),
                keyOfSlot,
                key
            );
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Index Setup(ref Meta setFor, Meta.Data setData, TKey setKey)
        {
            var keyIndex = this.count++;
            this.keys[keyIndex] = setKey;
            return (setFor = new(keyIndex, setData)).KeyIndex;
        }
    }

}
