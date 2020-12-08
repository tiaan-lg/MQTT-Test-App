using System;
using System.Collections.Generic;
using System.Text;

namespace MQTT
{
    /// <summary>
    /// Used as a fixed sized queue to dequeue when size is exceeded
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Buffer<T> : Queue<T>
    {
        private int Size { get; }

        public Buffer(int size)
        {
            Size = size;
        }

        /// <summary>
        /// Buffer Enqueue dequeues items if size is exceeded
        /// </summary>
        /// <returns></returns>
        public new void Enqueue(T obj)
        {
            base.Enqueue(obj);

            while (base.Count > Size)
            {
                base.Dequeue();
            }
        }
    }
}
