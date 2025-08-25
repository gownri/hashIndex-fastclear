using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace HashIndex.InternalModules;

internal static class HashTablePool
{

    private const int TableSizeFloor = 15;
    private static readonly bool isSupported =
        (Unsafe.SizeOf<Meta>() * TableSizeFloor) > Unsafe.SizeOf<BufferData>();
    private static readonly Guid guid = Guid.NewGuid();
    private static readonly BufferData defaultData = default;
    internal static Meta[] Rent(int minimumLength, out HashTableVersion tableVersion)
    {
        const int index = 0;
        minimumLength |= TableSizeFloor;
        
        var array = ArrayPool<Meta>.Shared.Rent(minimumLength);
        if (isSupported)
        {
            if ((uint)index >= (uint)array.Length)
            {
                tableVersion = HashTableVersion.Create();
                return array; 
            }
            ref var destination = ref Unsafe.As<Meta, byte>(ref array[index]);
            var bufferData = Unsafe.ReadUnaligned<BufferData>(ref destination);
            Unsafe.WriteUnaligned(ref destination, defaultData);
            if (bufferData == guid)
            {
                tableVersion = bufferData.version
                    .IncrementTable(out var overflowed)
                    .ResetGeneration();
                if(overflowed)
                    Array.Clear(array, 0, array.Length);
            } else
            {
                tableVersion = HashTableVersion.Create(); 
                Array.Clear(array, 0, array.Length);
            }
        }
        else
        {
            tableVersion = HashTableVersion.Create();
            Array.Clear(array, 0, array.Length);
        }

        return array;
    }

    internal static void Return(Meta[] table, HashTableVersion version)
    {
        if (TableSizeFloor > table.Length)
            return;
        ref var destination = ref Unsafe.As<Meta, byte>(ref table[0]);
        Unsafe.WriteUnaligned(ref destination, new BufferData(guid, version));
        ArrayPool<Meta>.Shared.Return(table);
    }

    private readonly struct BufferData
    {
        public readonly Guid guid;
        public readonly HashTableVersion version;

        public BufferData(Guid guid, HashTableVersion version)
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