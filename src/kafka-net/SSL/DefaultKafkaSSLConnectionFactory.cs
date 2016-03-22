using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class DefaultKafkaSSLConnectionFactory : DefaultKafkaConnectionFactory
    {
        private string _clientCertThumbprint;

        private string _nextClientCertThumbprint;

        public DefaultKafkaSSLConnectionFactory(string clientCertThumbprint, string nextClientCertThumbprint)
        {
            _clientCertThumbprint = clientCertThumbprint;
            _nextClientCertThumbprint = nextClientCertThumbprint;
        }

        public override IKafkaConnection Create(KafkaEndpoint endpoint, TimeSpan responseTimeoutMs, IKafkaLog log, TimeSpan? maximumReconnectionTimeout = null, BrokerRouter brokerRouter = null)
        {
            var kafkaSocket = new KafkaTcpSocket(log, endpoint, maximumReconnectionTimeout, true, this._clientCertThumbprint, this._nextClientCertThumbprint, true);
            if (brokerRouter != null)
            {
                kafkaSocket.OnServerUnreachable += (kafkaEndpoint) => brokerRouter.OnServerUnreachable(kafkaEndpoint);
            }
            return new KafkaConnection(kafkaSocket, responseTimeoutMs, log);
        }
    }
}
