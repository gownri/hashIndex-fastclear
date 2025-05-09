
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexers
{
    [StructLayout(LayoutKind.Auto)]
    public ref struct EntriesEnumerator
    {
        private Span<Meta> bucket;
        private readonly Meta.Data equality;
        private Index current;
        private int nextIndex;
        private readonly int jump;
        public readonly Context Current {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(this.current, new(nextIndex, equality)); 
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
#if DEBUG
            if (this.bucket.Length <= 0)
                throw new NullReferenceException(nameof(EntriesEnumerator));
            if (this.nextIndex < 0)
                throw new InvalidOperationException("this enumerator was disposed");
#endif

            if (this.bucket.Length == 0)
                return false;
            var temp = this.bucket.GetBucket(
                this.nextIndex,
                out var index
            );

            this.current = temp.KeyIndex;
            this.nextIndex = index + this.jump;
            return true;
        }
        internal EntriesEnumerator(Span<Meta> bucket, int entryIndex)
        {
            this.bucket = bucket;
            this.current = default;
            this.nextIndex = entryIndex;
            this.equality = bucket.GetBucket(nextIndex).MashedVDH;
            this.jump = (int)equality.GetJumpType();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            this.nextIndex = -1;
            this.bucket = default;
        }
    }

}
