using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    public class KafkaMessagePackerPool
    {
        private static KafkaMessagePackerPool instance = new KafkaMessagePackerPool();

        private ConcurrentQueue<KafkaMessagePacker> packerQueue = new ConcurrentQueue<KafkaMessagePacker>();
        public static KafkaMessagePackerPool Instance
        {
            get
            {
                return instance;
            }
        }

        public int Count
        {
            get
            {
                return packerQueue.Count;
            }
        }

        public KafkaMessagePacker Get()
        {
            if (packerQueue.IsEmpty)
            {
                return new KafkaMessagePacker();
            }
            else
            {
                KafkaMessagePacker packer = null;
                packerQueue.TryDequeue(out packer);
                if (packer != null)
                {
                    return packer;
                }
                else
                {
                    return new KafkaMessagePacker();
                }
            }
        }

        public void Return(KafkaMessagePacker packer)
        {
            this.packerQueue.Enqueue(packer);
        }
    }
}
