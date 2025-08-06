using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndexes.InternalModules;

internal static class BucketPool
{

    private const int BucketSizeFloor = 15;
    private static readonly bool isSupported =
        (Unsafe.SizeOf<Meta>() * BucketSizeFloor) > Unsafe.SizeOf<BufferData>();
    private static readonly Guid guid = Guid.NewGuid();
    private static readonly BufferData defaultData = default;
    internal static Meta[] Rent(int minimumLength, out BucketVersion bucketVersion)
    {
        const int index = 0;
        minimumLength |= BucketSizeFloor;
        
        var array = ArrayPool<Meta>.Shared.Rent(minimumLength);
        if (isSupported)
        {
            if ((uint)index >= (uint)array.Length)
            {
                bucketVersion = BucketVersion.Create();
                return array; 
            }
            ref var destination = ref Unsafe.As<Meta, byte>(ref array[0]);
            var bufferData = Unsafe.ReadUnaligned<BufferData>(ref destination);
            Unsafe.WriteUnaligned(ref destination, defaultData);
            if (bufferData == guid)
            {
                bucketVersion = bufferData.version;
                //bufferGuid.version
                //    .IncrementBucket(out var over)
                //    .ResetGeneration();
                //isManaged ^= over;
            } else
            {
                bucketVersion = BucketVersion.Create(); 
                Array.Clear(array, 0, array.Length);
            }
        }
        else
        {
            bucketVersion = BucketVersion.Create();
            Array.Clear(array, 0, array.Length);
        }

        return array;
    }

    internal static void Return(Meta[] bucket, BucketVersion version)
    {
        const int index = 0;
        if ((uint)index >= (uint)bucket.Length)
            return;
        ref var destination = ref Unsafe.As<Meta, byte>(ref bucket[index]);
        Unsafe.WriteUnaligned(ref destination, new BufferData(guid, version));
        ArrayPool<Meta>.Shared.Return(bucket);
    }

    private readonly struct BufferData
    {
        public readonly Guid guid;
        public readonly BucketVersion version;

        public BufferData(Guid guid, BucketVersion version)
        {
            this.guid = guid;
            this.version = version;
        }
        public static bool operator ==(in BufferData left, in BufferData right)
            => left.guid == right.guid;
        public static bool operator !=(in BufferData left, in BufferData right)
            => left.guid != right.guid;
        public static bool operator ==(in BufferData left, in Guid guid)
            => left.guid == guid;
        public static bool operator !=(in BufferData left, in Guid guid)
            => left.guid != guid;
        public readonly override bool Equals(object obj)
            => obj is BufferData my && this == my;
        public readonly override int GetHashCode()
            => this.guid.GetHashCode();
    }
}