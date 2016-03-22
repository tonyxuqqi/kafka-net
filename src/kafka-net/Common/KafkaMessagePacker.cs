using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    public class KafkaMessagePacker : IDisposable
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;
        private readonly MemoryStream ms;

        public KafkaMessagePacker()
        {
            ms = new MemoryStream();
            _stream = new BigEndianBinaryWriter(ms);
            Pack(IntegerByteSize); //pre-allocate space for buffer length
        }

        public long Length
        {
            get
            {
                return this.ms.Length;
            }
        }

        public KafkaMessagePacker Pack(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public KafkaMessagePacker Pack(Int32 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int16 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int64 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(byte[] buffer, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(buffer, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(byte[] buffer, int offset, int count, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(buffer, offset, count, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(data, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(IEnumerable<string> data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            foreach (var item in data)
            {
                _stream.Write(item, encoding);
            }

            return this;
        }

        public async Task WritePayloadAsync(Stream stream)
        {
            long currentPosition = _stream.BaseStream.Position;
            _stream.BaseStream.Position = 0;
            Pack((Int32)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = currentPosition;
            await stream.WriteAsync(this.ms.GetBuffer(), 0, (int)this.ms.Length);
        }

        public void WritePayload(KafkaMessagePacker packer)
        {
            long currentPosition = _stream.BaseStream.Position;
            _stream.BaseStream.Position = 0;
            Pack((Int32)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = currentPosition;
            packer.Pack(this.ms.GetBuffer(), 0, (int)this.ms.Length);
        }

        public async Task WriteCrcPayloadAsync(Stream stream)
        {
            var buffer = this.ms.GetBuffer();

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, (int)this.ms.Length); // not buffer.Length as buffer has free space.
            buffer[0] = crc[0];
            buffer[1] = crc[1];
            buffer[2] = crc[2];
            buffer[3] = crc[3];

            await stream.WriteAsync(this.ms.GetBuffer(), 0, (int)this.ms.Length);
        }

        public void WriteCrcPayload(KafkaMessagePacker packer)
        {
            var buffer = this.ms.GetBuffer();

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, (int)this.ms.Length); // not buffer.Length as buffer has free space.
            buffer[0] = crc[0];
            buffer[1] = crc[1];
            buffer[2] = crc[2];
            buffer[3] = crc[3];

            packer.Pack(this.ms.GetBuffer(), 0, (int)this.ms.Length);
        }

        public async Task WritePayloadNotLengthAsync(Stream stream)
        {
            await stream.WriteAsync(this.ms.GetBuffer(), IntegerByteSize, (int)this.ms.Length - IntegerByteSize);
        }

        public byte[] Payload()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Pack((Int32)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);
            return buffer;
        }

        public byte[] PayloadNoLength()
        {
            var payloadLength = _stream.BaseStream.Length - IntegerByteSize;
            var buffer = new byte[payloadLength];
            _stream.BaseStream.Position = IntegerByteSize;
            _stream.BaseStream.Read(buffer, 0, (int)payloadLength);
            return buffer;
        }

        public byte[] CrcPayload()
        {
            var buffer = new byte[_stream.BaseStream.Length];

            //copy the payload over
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, buffer.Length);
            buffer[0] = crc[0];
            buffer[1] = crc[1];
            buffer[2] = crc[2];
            buffer[3] = crc[3];

            return buffer;
        }

        public void Reset()
        {
            this._stream.Seek(IntegerByteSize, SeekOrigin.Begin);
            this._stream.BaseStream.SetLength(IntegerByteSize);
        }

        public void Dispose()
        {
            using (_stream) { }
        }
    }
}