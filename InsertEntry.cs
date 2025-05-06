using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace HashIndexers
{
    public readonly ref struct InsertEntry
    {
        internal readonly Span<Meta> bucket;
        internal readonly Meta.Data equality;
        internal readonly int entryIndex;
        internal readonly int JumpLength;

        internal InsertEntry(Span<Meta> bucket, Meta.Data equality, int entryIndex, int jumpLength)
        {
            this.bucket = bucket;
            this.equality = equality;
            this.entryIndex = entryIndex;
            this.JumpLength = jumpLength;
        }
        public readonly EntriesEnumerator GetEnumerator()
            => new(this.bucket, this.equality, this.entryIndex, this.JumpLength);

    }


    public readonly struct Context
    {
        public readonly Index KeyIndex;
        internal readonly InsertHint hint;

        internal Context(Index keyIndex, InsertHint hint)
        {
            this.KeyIndex = keyIndex;
            this.hint = hint;
        }
        public readonly void Deconstruct(out Index index, out InsertHint hint)
        {
            index = this.KeyIndex;
            hint = this.hint;
        }

        public static implicit operator Index(Context context)
            => context.KeyIndex;
    }

    public readonly struct InsertHint
    {
        private readonly int index;
        internal readonly Meta.Data metaData;
        internal readonly int Index 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)] 
            get => unchecked(this.index - 1); 
        }
        public readonly bool IsValid => this.index != default;
        internal InsertHint(int index, Meta.Data data)
        {
            this.index = unchecked(index + 1); 
            this.metaData = data;
        }
    }
}
