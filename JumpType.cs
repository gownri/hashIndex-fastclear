using System.Runtime.CompilerServices;

namespace HashIndexes
{
    internal readonly struct JumpType
    {
        internal readonly uint ForMetaDataDistanceAddOperationValue;
        internal JumpType(Meta.Data data)
            => this.ForMetaDataDistanceAddOperationValue = (uint)((((data.RawData & (-data.RawData)) & 0b0011) << 1) + 1) << Meta.Data.DistanceOffset;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator int(JumpType type)
            => (int)(type.ForMetaDataDistanceAddOperationValue >> Meta.Data.DistanceOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetJumpLength(uint rawData)
            => (int)((((rawData & (-rawData)) & 0b0011) << 1) + 1);
        public readonly override string ToString()
            => $"Jump:{(int)this,3}";
        
    }

}
