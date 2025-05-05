#nullable enable
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
namespace HashIndexers
{


    public class HashIndex<TKey>
        where TKey : notnull, IEquatable<TKey>
    {
        internal void DebugDisplay()
        {
            for (var i = 0; i < this.hashBucket.Length; ++i)
            {
                ref var meta = ref this.hashBucket[i];
                Console.WriteLine($" index = {i, 3} : key = {(meta.KeyIndex < 0 ? "null": this.keys[meta.KeyIndex]), 12} [ {meta} ]");
            }
        }
        private Version version;
        private Meta[] hashBucket;
        private TKey[] keys;

        private int count = 0;
        private int maxCollisionDistance = 0;
        public ReadOnlySpan<TKey> Keys => this.keys.AsSpan(0, this.count);
        
        public HashIndex(int initSize)
        {
            this.version = Version.Create();
            int size = 1;
            while ( (size <<= 1) < initSize );

            this.hashBucket = new Meta[size];
#if DEBUG
            this.hashBucket.AsSpan().Fill(new(unchecked((int)0xABADBEEF), default));
#endif
            this.keys = new TKey[initSize];
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
            if (this.count >= this.hashBucket.Length 
                || this.maxCollisionDistance > Meta.Data.MaxCountableDistance)
                this.BucketExpand();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetBucketIndex(int hash)
            => hash & (this.hashBucket.Length - 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetBucketIndex(int hash, int bucketLength)
            => hash & (bucketLength - 1);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetIndex(TKey key,[MaybeNullWhen(false)] out Index index)
        {
            var hashIndex = key.GetHashCode();
            var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
            hashIndex = this.GetBucketIndex(hashIndex);
            ref var meta = ref this.hashBucket[hashIndex];
            if (meta.RawData == entryKey.RawData 
                && this.keys[meta.KeyIndex].Equals(key))
            {
                index = meta.KeyIndex;
                return true;
            }
            var jump = entryKey.GetJumpType();
            hashIndex = this.GetBucketIndex(hashIndex + (int)jump);
            meta = ref this.hashBucket[hashIndex];
            entryKey = entryKey.AddJump(jump);
            if (meta.RawData == entryKey.RawData
                && this.keys[meta.KeyIndex].Equals(key))
            {
                index = meta.KeyIndex;
                return true;
            }
            return Find(hashIndex, entryKey, key, jump, out index);

            bool Find(int start, Meta.Data entry, TKey key, JumpType jumpType, out Index index)
            {
                index = this.FindOrLess(
                    start,
                    entry,
                    key,
                    jumpType,
                    out var exist,
                    out _,
                    out _
                ).KeyIndex;

                return exist;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Index GetIndex(TKey key, out bool exist, bool isExpandable)
        {
            if(isExpandable)
                this.BucketCheck();
            return this.GetIndex(key, out exist);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Index GetIndex(TKey key, out bool exist)
        {
#if DEBUG
            if (this.hashBucket.Length <= this.count)
                throw new InvalidOperationException($"{nameof(this.hashBucket)} is full");
#endif

            var hashIndex = key.GetHashCode();
            var entryKey = Meta.Data.CreateEntry(hashIndex, this.version.Bucket);
            hashIndex = this.GetBucketIndex(hashIndex);
            if (this.hashBucket[hashIndex].MashedVDH.Version != this.version.Bucket){
                exist = false;
                return this.Setup(ref this.hashBucket[hashIndex], entryKey, key);
            }

            var entry = this.FindOrLess(
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
                ref this.Insert(index, keyOfSlot),
                keyOfSlot,
                key
            );
        }

       
        private Index Setup(ref Meta setFor, Meta.Data setData, TKey setKey)
        {
            var keyIndex = this.count++;
            this.keys[keyIndex] = setKey;
            return (setFor = new(keyIndex, setData)).KeyIndex;
        }

        private ref Meta FindOrLess(int start, Meta.Data entry, TKey key, JumpType jumpType, 
           scoped out bool exist, scoped out int index, scoped out Meta.Data keyOfSlot)
        {
#if DEBUG
            if ((uint)start >= (uint)this.hashBucket.Length)
                throw new ArgumentOutOfRangeException(nameof(start));
#endif
            
            var jump = (int)jumpType;

            var bucket = (Span<Meta>)this.hashBucket;
            var keys = this.keys;
            var distanceLimit = Math.Min(Meta.Data.MaxCountableDistance-entry.Distance, bucket.Length);
            var span = Span<Meta>.Empty;
            int pos;
#if !DEBUG
            ref var current = ref Unsafe.NullRef<Meta>();
#endif
            do
            {
                //span = bucket.AsSpan(start, Math.Min(distanceLimit, bucket.Length-start) );
                span = bucket.Slice( start, Math.Min(distanceLimit, bucket.Length - start) );
                for (pos = 0; pos < span.Length; pos += jump)
                {
#if DEBUG
                    ref var
#endif
                            current = ref span[pos];
                    if (  ( (exist = current.RawData == entry.RawData)
                            && !keys[current.KeyIndex].Equals(key) )
                        || current.RawData > entry.RawData  )
                    //if ( current.RawData > entry.RawData 
                    //    || (exist = current.RawData == entry.RawData)
                    //        && !keys[current.KeyIndex].Equals(key)
                    //    )
                    {
                        entry = entry.AddJump(jumpType);
                        continue;
                    }
                    this.maxCollisionDistance = Math.Max(entry.Distance, this.maxCollisionDistance);
                    index = pos + start;
                    keyOfSlot = entry;
                    return ref current;
                }
                distanceLimit -= pos;
                start = GetBucketIndex(pos + start, bucket.Length);
            } while (distanceLimit > 0);

            return ref ProbeOverWork(bucket, out exist, out index, out keyOfSlot);
            [MethodImpl(MethodImplOptions.NoInlining)]
            ref Meta ProbeOverWork(Span<Meta> span, out bool exist, out int index, out Meta.Data keyOfSlot)
            {
                var overProveCount = Meta.Data.MaxCountableDistance;
                pos = start;
                
#if DEBUG
                if (span.Length < Meta.Data.MaxCountableDistance || span.Length <= this.count)
                    throw new System.IO.InternalBufferOverflowException(nameof(this.hashBucket));
                for (var safe = 0; safe < span.Length; ++safe)
#else
                while(true)
#endif
                {
                    ref var current = ref span[pos];
                    if (((exist = current.RawData == entry.RawData)
                            && !this.keys[current.KeyIndex].Equals(key))
                        || current.RawData > entry.RawData)
                    {
                        pos = this.GetBucketIndex(pos + jump);
                        overProveCount += jump;
                        entry = entry.AddJump(jumpType);
                        continue;
                    }
                    this.maxCollisionDistance = Math.Max(overProveCount, this.maxCollisionDistance);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref Meta Insert(int insertStartIndex, Meta.Data insertKey)
        {
            ref var container = ref this.hashBucket[insertStartIndex];
#if DEBUG
            if((uint)insertStartIndex >= (uint)this.hashBucket.Length)
                throw new ArgumentOutOfRangeException(nameof(insertStartIndex));
            if (this.hashBucket.Length <= this.count)
                throw new InvalidOperationException($"{nameof(this.hashBucket)} is full");
#endif
            long bucketVersion = (uint)this.version.Bucket << Meta.Data.VersionOffset;
            if ( ((long)container.RawData - (long)bucketVersion) >= 0 )
                ShiftInsert(this.hashBucket, version, bucketVersion, insertStartIndex, container);
            container = Meta.Create(insertKey);
            return ref container;

            static void ShiftInsert(Span<Meta> bucket, Version version, long bucketVersion, int insertStartIndex, Meta insertItem)
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
                    for(var safe = 0; safe < bucket.Length; ++safe)
#else
                    while (true)
#endif
                    {
                        insertPoint = ref bucket[GetBucketIndex(insertStartIndex += jump, bucket.Length)];
                        if (limit > 0)
                            current = current.AddJump(jumpType);
                        //if (insertPoint.MashedVDH.Version != this.version.Bucket)
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
       
        //internal enum JumpType : int
        //{
        //    One = 1,
        //    Short = 3,
        //    Medium = 5,
        //    Long = 7,
        //}

       

        
    }

}
