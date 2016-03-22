using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    public class KafkaMessagePooledPacker : IDisposable
    {
        KafkaMessagePacker packer;

        public KafkaMessagePooledPacker()
        {
            packer = KafkaMessagePackerPool.Instance.Get();
        }

        public long Length
        {
            get
            {
                return packer.Length;
            }
        }

        public KafkaMessagePooledPacker Pack(byte value)
        {
            packer.Pack(value);
            return this;
        }

        public KafkaMessagePooledPacker Pack(Int32 ints)
        {
            packer.Pack(ints);
            return this;
        }

        public KafkaMessagePooledPacker Pack(Int16 ints)
        {
            packer.Pack(ints);
            return this;
        }

        public KafkaMessagePooledPacker Pack(Int64 ints)
        {
            packer.Pack(ints);
            return this;
        }

        public KafkaMessagePooledPacker Pack(byte[] buffer, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            packer.Pack(buffer, encoding);
            return this;
        }

        public KafkaMessagePooledPacker Pack(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            packer.Pack(data, encoding);
            return this;
        }

        public KafkaMessagePooledPacker Pack(IEnumerable<string> data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            packer.Pack(data, encoding);

            return this;
        }

        public async Task WritePayloadAsync(Stream stream)
        {
            await packer.WritePayloadAsync(stream);
        }

        public void WritePayload(KafkaMessagePooledPacker targetPacker)
        {
            packer.WritePayload(targetPacker.packer);
        }

        public async Task WriteCrcPayloadAsync(Stream stream)
        {
            await packer.WriteCrcPayloadAsync(stream);
        }

        public void WriteCrcPayload(KafkaMessagePooledPacker targetPacker)
        {
            packer.WriteCrcPayload(targetPacker.packer);
        }

        public async Task WritePayloadNotLengthAsync(Stream stream)
        {
            await packer.WritePayloadNotLengthAsync(stream);
        }

        public byte[] Payload()
        {
            return packer.Payload();
        }

        public byte[] PayloadNoLength()
        {
            return packer.PayloadNoLength();
        }

        public byte[] CrcPayload()
        {
            return packer.CrcPayload();
        }

        public void Dispose()
        {
            packer.Reset();
            KafkaMessagePackerPool.Instance.Return(packer);
        }
    }
}
