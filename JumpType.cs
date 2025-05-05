using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace HashIndexers
{
    internal readonly struct JumpType
    {
        internal readonly uint ForMetaDataDistanceAddOperationValue;
        internal JumpType(Meta.Data data)
            => this.ForMetaDataDistanceAddOperationValue = (uint)((((data.RawData & (-data.RawData)) & 0b0011) << 1) + 1) << Meta.Data.DistanceOffset;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator int(JumpType type)
            => (int)(type.ForMetaDataDistanceAddOperationValue >> Meta.Data.DistanceOffset);

        public readonly override string ToString()
            => $"{(int)this,3} : {this.ForMetaDataDistanceAddOperationValue,8:X}";
    }
}
