
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexers
{
    public ref struct EntriesEnumerator
    {
        private readonly Span<Meta> bucket;
        private readonly Meta.Data equality;
        private Index current;
        private int nextIndex;
        private readonly int jump;
        public readonly Context Current => new(this.current, new(nextIndex, equality));
        public bool MoveNext()
        {
#if DEBUG
            if (this.bucket.Length <= 0)
                throw new NullReferenceException(nameof(EntriesEnumerator));
            if (this.nextIndex < 0)
                throw new InvalidOperationException("this enumerator was disposed");
#endif
            var temp = this.bucket.GetBucket(
                this.nextIndex, 
                out var index
            );
            if (temp.RawData != this.equality.RawData)
                return false;
            this.current = temp.KeyIndex;
            this.nextIndex = index + this.jump;
            return true;
        }
        internal EntriesEnumerator(Span<Meta> bucket, Meta.Data equality, int entryIndex, int jumpLength)
        {
            this.bucket = bucket;
            this.current = default;
            this.nextIndex = entryIndex;
            this.equality = equality;
            this.jump = jumpLength;
        }

        internal EntriesEnumerator(Span<Meta> bucket, int entryIndex)
            : this(bucket, bucket[entryIndex].MashedVDH, entryIndex, (int)bucket[entryIndex].MashedVDH.GetJumpType())
        {

        }


        public void Dispose()
        {
            this.nextIndex = -1;
        }
    }

}
