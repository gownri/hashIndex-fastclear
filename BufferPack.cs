using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace HashIndexers
{
    public class BufferPack
    {
        private readonly Meta[] array;
        private readonly int size;
        internal BucketVersion version;
        private bool isRented = false;
        private bool isDisposed = false;
        public BufferPack(int size)
        {
            this.version = BucketVersion.Create();
            this.size = Helper.GetNextPowerOfTwo(size);
            this.array = ArrayPool<Meta>.Shared.Rent(size);
        }

        internal Span<Meta> Rent()
        {
            this.isRented = true;
            return new Span<Meta>(this.array, 0, this.size);
        }

        public void Return<TKey>(UnitHashIndex unit)
            where TKey : notnull, IEquatable<TKey>
        {
            var returnBucket = unit.BucketSource;
            if (Unsafe.AreSame(ref this.array[0], ref unit.BucketSource[0]) )
            {
                this.version = unit.Version;
                this.isRented = false;
            }
            else
            {
                throw new InvalidOperationException("BufferPack is not the owner of the buffer");
            }
            if (this.isDisposed)
                ArrayPool<Meta>.Shared.Return(this.array);
        }

        public void Dispose()
        {
            if (!this.isRented)
            {
                ArrayPool<Meta>.Shared.Return(this.array);
            }
        }
    }
}
