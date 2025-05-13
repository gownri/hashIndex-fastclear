using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexes
{
    //[StructLayout(LayoutKind.Auto)]
    public readonly ref struct Entry
    {
        internal readonly Span<Meta> bucket;
        internal readonly int entryIndex;
        internal readonly Meta.Data equality;

        public readonly bool HasEntry => this.bucket.Length > 0;

        internal Entry(Span<Meta> bucket, int entryIndex, Meta.Data equality)
        {
            this.bucket = bucket;
            this.entryIndex = entryIndex;
            this.equality = equality;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryPeekInitEntry([MaybeNullWhen(true)] out InsertHint insertHint)
        {
            var ret = this.bucket.Length != 0
                        && this.bucket.GetBucket(this.entryIndex).RawData == this.equality.RawData;
            insertHint = ret ? default : new(this.entryIndex, this.equality);
            return ret;
        }
        public readonly EntriesEnumerator GetEnumerator()
            => this.bucket.Length == 0
                ? default
                : new(
                    this.bucket,
                    this.entryIndex
                );
    }

    public readonly struct Context
    {
        internal readonly Index keyIndex;
        internal readonly InsertHint insertHint;

        public readonly Index KeyIndex => this.keyIndex;
        public readonly InsertHint InsertHint => this.insertHint;

        internal Context(Index keyIndex, InsertHint hint)
        {
            this.keyIndex = keyIndex;
            this.insertHint = hint;
        }
        public readonly void Deconstruct(out Index index, out InsertHint hint)
        {
            index = this.keyIndex;
            hint = this.insertHint;
        }

        public static implicit operator Index(Context context)
            => context.keyIndex;
        public static implicit operator InsertHint(Context context)
            => context.insertHint;
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
