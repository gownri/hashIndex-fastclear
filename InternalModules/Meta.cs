using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HashIndex.InternalModules;

internal readonly struct Meta
{
    internal static readonly Meta Sentinel = new(unchecked((int)0xBEEFF00D), Data.Sentinel);
    internal readonly int KeyIndex;
    internal readonly uint RawData;

    public readonly Data MashedVDH {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get =>
        Data.FromUInt(this.RawData);
        //Unsafe.As<uint, Meta.Data>(ref Unsafe.AsRef(this.RawData));
    }
    internal Meta(int keyIndex, Data data)
    {
        this.KeyIndex = keyIndex;
        this.RawData = data.RawData;
    }
    private Meta(int keyIndex, uint raw)
    {
        this.KeyIndex = keyIndex;
        this.RawData = raw;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal readonly Meta Update(Data data)
        => new(this.KeyIndex, data);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal readonly Meta UpdateKeyIndex(int keyIndex)
        => new(keyIndex, this.RawData);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Meta Create(Data data)
        => new(-1, data.RawData);
    public override string ToString()
        => $"keyIndex = {(this.KeyIndex < 0 ? this.KeyIndex.ToString("X") : this.KeyIndex),8} : Data = {this.MashedVDH}";
    public readonly struct Data
    {
        internal static readonly Data Sentinel = new(0xABADBEEF);
        internal static readonly Data Initial = CreateEntry(0, 1);
        internal const int DistanceOffset = sizeof(byte) * 8;
        internal const int VersionOffset = sizeof(byte) * 8 + sizeof(byte) * 8;
        internal const int HashMask = 0xFF;
        internal readonly uint RawData;

        public readonly int Distance
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(this.RawData >> DistanceOffset & 0xFF);
        }
        public readonly int Version
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(this.RawData >> VersionOffset);
        }

        public readonly int Hash
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(this.RawData & HashMask);
        }
        internal const int MaxCountableDistance = byte.MaxValue;
        private Data(uint hash, ushort version)
        {
            uint mash = (hash >> 8) ^ hash;
            mash ^= mash >> 16;
            mash &= 0xFF;
            //uint mash = hash ^ (hash << 8);
            //mash ^= mash << 16;
            //mash >>= 24;
            this.RawData = (uint)version << VersionOffset | mash;
        }

        private Data(uint data)
            => this.RawData = data;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Data AddJump(JumpType jt)
            => new(unchecked(this.RawData + jt.ForMetaDataDistanceAddOperationValue));
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly JumpType GetJumpType()
            => new(this);
        //(JumpType)(  ( unchecked((short)this.RawData & -(short)this.RawData) & 0b0110 ) + 1); //{1, 3, 5}
        //(JumpType)((this.RawData & 0b0110) + 1);// {1, 3, 5, 7}
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly Data SetDistance(int distance)
        {
#if DEBUG
            if((uint)distance > (uint)byte.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(distance), $"Distance must be in range [0, {byte.MaxValue}]");
#endif
            const int distanceMask = byte.MaxValue << DistanceOffset;
            return unchecked(  new((this.RawData & (uint)~distanceMask) | ((uint)distance << DistanceOffset))  );
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Data CreateEntry(int hash, ushort version)
            => new((uint)hash, version);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Data FromUInt(uint data)
            => new(data);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly uint ToUInt()
            => this.RawData;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly Meta.Data SubJump(JumpType jt)
            => new(unchecked(this.RawData - jt.ForMetaDataDistanceAddOperationValue));

        public override string ToString()
            => $"{{ hash:{this.RawData & 0xFF,3} v:{this.Version,3} d:{this.Distance,3} j:{(int)this.GetJumpType(),2} }}";

    }
}
