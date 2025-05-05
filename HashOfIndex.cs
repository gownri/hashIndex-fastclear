using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace HashIndexers
{
    public ref struct HashOfIndex<TKey>
        where TKey : notnull, IEquatable<TKey>
    {
        private Span<Meta> bucket;
        private int count;
        private int version;
    }

    public ref struct IndexEnumerator<TKey>
    {
    }
}
