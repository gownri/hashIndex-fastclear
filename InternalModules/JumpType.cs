using System.Runtime.CompilerServices;
using static HashIndex.InternalModules.Meta;

namespace HashIndex.InternalModules;

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
        => this.ForMetaDataDistanceAddOperationValue 
        = (uint)((data.RawData & (-data.RawData) & 0b0110) | 1) << Meta.Data.DistanceOffset;

    private JumpType(uint raw)
        => this.ForMetaDataDistanceAddOperationValue = raw;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetJumpLength(uint rawData)
        => 
        (int)((rawData & (-rawData) & 0b0110) | 1);
    public readonly override string ToString()
        => $"Jump:{(int)this,3}";

    /// <returns>{1, 3, 5}</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static explicit operator int(JumpType type)
        => (int)(type.ForMetaDataDistanceAddOperationValue >> Meta.Data.DistanceOffset) | 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static JumpType operator *(JumpType self, int multiplier)
        => new((uint)self.ForMetaDataDistanceAddOperationValue * (uint)multiplier);
}
