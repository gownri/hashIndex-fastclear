
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexes
{
    [StructLayout(LayoutKind.Auto)]
    public ref struct EntriesEnumerator
    {
        private Span<Meta> bucket;
        private readonly Meta.Data equality;
        private int currentIndex;
        public readonly Context Current {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { 
                var temp = this.bucket.GetBucket(this.currentIndex);
                return new Context(
                        temp.KeyIndex,
                        new(
                            this.currentIndex,
                            temp.MashedVDH
                        )
                );
            } 
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (this.bucket.Length == 0)
                return false;
            var jump = JumpType.GetJumpLength(this.equality.RawData);
            var temp = this.bucket.GetBucket(
                this.currentIndex + jump,
                out var index
            );

            if(temp.RawData != this.equality.RawData)
            {
                this.bucket = default;
            }
            this.currentIndex = index;
            return true;
        }
        internal EntriesEnumerator(Span<Meta> bucket, int entryIndex)
        {
            this.bucket = bucket;
            if (this.bucket.Length == 0)
                return;
            this.currentIndex = entryIndex;
            this.equality = bucket.GetBucket(this.currentIndex).MashedVDH;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            this.bucket = default;
        }
    }

}
