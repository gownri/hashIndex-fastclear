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
        const int insertEntryFlag = int.MinValue;
        internal readonly Span<Meta> bucket;
        internal readonly Meta.Data equality;
        internal readonly int entryIndex;
        private readonly int JumpLength;
        public readonly long Dummy = 1;
        public readonly long Dummy2 = 2;

        internal Entry(Span<Meta> bucket, Meta.Data equality, int entryIndex, int jumpLength, bool isInsertEntry)
        {
            this.bucket = bucket;
            this.equality = equality;
            this.entryIndex = entryIndex;
            this.JumpLength = jumpLength | (!isInsertEntry ? insertEntryFlag : 0);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //[MethodImpl(MethodImplOptions.NoInlining)]
        public readonly bool TryReadInitExist([MaybeNullWhen(true)]out InsertHint insertHint)
        {
            insertHint = new(entryIndex, equality);
            return (this.JumpLength & insertEntryFlag) != 0;
        }
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public readonly EntriesEnumerator GetEnumerator()
            => new(this.bucket, this.equality, this.entryIndex, this.JumpLength & ~insertEntryFlag);
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
