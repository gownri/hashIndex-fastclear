using System.Runtime.CompilerServices;

namespace HashIndexes.InternalModules;

internal readonly struct JumpType
{
    /// <summary>
    /// jumpLength = 1
    /// </summary>
    public readonly static JumpType None = default;

    internal readonly uint ForMetaDataDistanceAddOperationValue;

    public readonly bool IsNone
        => this.ForMetaDataDistanceAddOperationValue == 0;

    internal JumpType(Meta.Data data)
        => this.ForMetaDataDistanceAddOperationValue = (uint)(((data.RawData & -data.RawData & 0b0011) << 1) + 1) << Meta.Data.DistanceOffset;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetJumpLength(uint rawData)
        => (int)(((rawData & -rawData & 0b0011) << 1) + 1);
    public readonly override string ToString()
        => $"Jump:{(int)this,3}";

    /// <returns>{1, 3, 5}</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static explicit operator int(JumpType type)
        => (int)(type.ForMetaDataDistanceAddOperationValue >> Meta.Data.DistanceOffset) | 1;
}
