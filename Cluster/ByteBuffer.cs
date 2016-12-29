using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    public class ByteBuffer : Queue<byte>
    {
        public void Enqueue(byte[] values, int offset, int count)
        {
            for (int i = 0; i < count; ++i)
            {
                Enqueue(values[offset + i]);
            }
        }

        public int Dequeue(byte[] bytes)
        {
            int maxBytes = System.Math.Min(Count, bytes.Length);
            for (int i = 0; i < maxBytes; i++)
            {
                bytes[i] = Dequeue();
            }
            return maxBytes;
        }

        /*
                private byte[] _buffer;
                private int _tail;
                private int _head;
                private int _count;
                private int _capacity;

                private const int _defaultCapacity = 4096;

                public ByteBuffer()
                {
                    _buffer = new byte[_defaultCapacity];
                    _capacity = _defaultCapacity;
                    _tail = 0;
                    _head = 0;
                    _count = 0;
                }

                public int Count
                {
                    get { return _count; }
                }

                public void Clear()
                {
                    _count = 0;
                    _head = _tail;
                }

                public void Enqueue(byte value)
                {
                    if (_count == _capacity)
                    {
                        Grow();
                    }
                    _buffer[_tail] = value;
                    _tail = (_tail + 1) % _capacity;
                    _count++;
                }

                public void Enqueue(byte[] values, int offset, int count)
                {
                    if (count > (_capacity - _count)) EnsureCapacity(count + _count);

                    for(int i = 0; i < count; ++i)
                    {
                        Enqueue(values[offset + i]);
                    }
                }

                public byte Dequeue()
                {
                    if (_count == 0) throw new InvalidOperationException("Queue is empty");
                    byte value = _buffer[_head];
                    _head = (_head + 1) % _capacity;
                    _count--;
                    return value;
                }

                public int Dequeue(byte[] bytes)
                {
                    int maxBytes = System.Math.Min(_count, bytes.Length);
                    for (int i = 0; i < maxBytes; i++)
                    {
                        bytes[i] = Dequeue();
                    }
                    return maxBytes;
                }

                private void Grow()
                {
                    int newCapacity = _capacity << 1;
                    EnsureCapacity(newCapacity);
                }

                private void EnsureCapacity(int newCapacity)
                {
                    // Round the capcity to the next power of 2
                    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
                    --newCapacity;
                    newCapacity |= newCapacity >> 1;
                    newCapacity |= newCapacity >> 2;
                    newCapacity |= newCapacity >> 4;
                    newCapacity |= newCapacity >> 8;
                    newCapacity |= newCapacity >> 16;
                    ++newCapacity;

                    byte[] newBuffer = new byte[newCapacity];

                    if (_head < _tail)
                    {
                        Buffer.BlockCopy(_buffer, _head, newBuffer, 0, _count);
                    }
                    else
                    {
                        Buffer.BlockCopy(_buffer, _head, newBuffer, 0, _capacity - _head);
                        Buffer.BlockCopy(_buffer, 0, newBuffer, _capacity - _head, _tail);
                    }

                    _buffer = newBuffer;
                    _tail = _count;
                    _head = 0;
                    _capacity = newCapacity;
                }
        */
    }
}
