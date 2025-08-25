using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace HashIndex.InternalModules;

public readonly struct ProveContext
{
    internal readonly int Hash;
    internal ProveContext(int hash, bool isInitialProved)
    {
        //this.Hash = isInitialProved ? hash : Test(hash);
        this.Hash = hash;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override readonly int GetHashCode() => HashCode.Combine(Hash);

}