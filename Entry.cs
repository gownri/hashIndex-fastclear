using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexers
{
    //[StructLayout(LayoutKind.Auto)]
    public readonly ref struct Entry
    {
        internal readonly Span<Meta> bucket;
        internal readonly int entryIndex;

        internal Entry(Span<Meta> bucket, int entryIndex)
        {
            this.bucket = bucket;
            this.entryIndex = entryIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryPeekInitEntry([MaybeNullWhen(true)] out InsertHint insertHint)
        {
            if(entryIndex < 0){
                insertHint = default;
                return false;
            }
            else
                insertHint = default;
            return true;
        }
        public readonly EntriesEnumerator GetEnumerator()
            => this.entryIndex < 0
            ? default
            : new(
                bucket,
                this.entryIndex
            );
    }

    public readonly struct Context
    {
        public readonly Index KeyIndex;
        public readonly InsertHint InsertHint;

        internal Context(Index keyIndex, InsertHint hint)
        {
            this.KeyIndex = keyIndex;
            this.InsertHint = hint;
        }
        public readonly void Deconstruct(out Index index, out InsertHint hint)
        {
            index = this.KeyIndex;
            hint = this.InsertHint;
        }

        public static implicit operator Index(Context context)
            => context.KeyIndex;
        public static implicit operator InsertHint(Context context)
            => context.InsertHint;
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
