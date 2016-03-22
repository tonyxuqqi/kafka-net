using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using KafkaNet.Statistics;


namespace KafkaNet
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event Action OnServerDisconnected;
        public event Action<KafkaEndpoint> OnServerUnreachable;  // action called when ServerUnreachableException occurs
        public event Action<int> OnReconnectionAttempt;
        public event Action<int> OnReadFromSocketAttempt;
        public event Action<int> OnBytesReceived;
        public event Action<KafkaDataPayload> OnWriteToSocketAttempt;

        private const int DefaultReconnectionTimeout = 100;
        private const int DefaultReconnectionTimeoutMultiplier = 2;
        private const int MaxReconnectionTimeoutMinutes = 5;

        private static X509Certificate2Collection clientCerts;
        private static X509Certificate2Collection nextClientCerts;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private readonly IKafkaLog _log;
        private readonly KafkaEndpoint _endpoint;
        private readonly TimeSpan _maximumReconnectionTimeout;

        private readonly AsyncCollection<SocketPayloadSendTask> _sendTaskQueue;
        private readonly AsyncCollection<SocketPayloadReadTask> _readTaskQueue;

        private readonly Task _socketTask;
        private readonly AsyncLock _clientLock = new AsyncLock();
        private TcpClient _client;
        private SslStream _sslStream;
        private int _disposeCount;
        private bool _enableSSL;
        private string _privateCertThumbprint;
        private string _nextPrivateCertThumbprint;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="maximumReconnectionTimeout">The maximum time to wait when backing off on reconnection attempts.</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, TimeSpan? maximumReconnectionTimeout = null, bool enableSSL = false, string privateCertThumbprint = null, string nextPrivateCertThumbprint = null, bool waitingForConnect = false)
        {
            _log = log;
            _endpoint = endpoint;
            _maximumReconnectionTimeout = maximumReconnectionTimeout ?? TimeSpan.FromMinutes(MaxReconnectionTimeoutMinutes);
            _enableSSL = enableSSL;
            _privateCertThumbprint = privateCertThumbprint;
            _nextPrivateCertThumbprint = nextPrivateCertThumbprint;

            _sendTaskQueue = new AsyncCollection<SocketPayloadSendTask>();
            _readTaskQueue = new AsyncCollection<SocketPayloadReadTask>();

            if (waitingForConnect)
            {
                if (!this.GetStream())
                {
                    throw new ServerDisconnectedException("Unable to connect to the broker {0}", endpoint.ServeUri.ToString());
                }
            }

            //dedicate a long running task to the read/write operations
            _socketTask = Task.Factory.StartNew(DedicatedSocketTask, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);

            _disposeRegistration = _disposeToken.Token.Register(() =>
            {
                _sendTaskQueue.CompleteAdding();
                _readTaskQueue.CompleteAdding();
            });

        }

        #region Interface Implementation...
        /// <summary>
        /// The IP Endpoint to the server.
        /// </summary>
        public KafkaEndpoint Endpoint { get { return _endpoint; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize)
        {
            return EnqueueReadTask(readSize, CancellationToken.None);
        }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return EnqueueReadTask(readSize, cancellationToken);
        }

        /// <summary>
        /// Convenience function to write full buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<KafkaDataPayload> WriteAsync(KafkaDataPayload payload)
        {
            return WriteAsync(payload, CancellationToken.None);
        }

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<KafkaDataPayload> WriteAsync(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            return EnqueueWriteTask(payload, cancellationToken);
        }
        #endregion

        private Task<KafkaDataPayload> EnqueueWriteTask(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            var sendTask = new SocketPayloadSendTask(payload, cancellationToken);
            _sendTaskQueue.Add(sendTask);
            StatisticsTracker.QueueNetworkWrite(_endpoint, payload);
            return sendTask.Tcp.Task;
        }

        private Task<byte[]> EnqueueReadTask(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReadTask(readSize, cancellationToken);
            _readTaskQueue.Add(readTask);
            return readTask.Tcp.Task;
        }

        private bool GetStream()
        {
            bool connected = false;
            try
            {
                //block here until we can get connections then start loop pushing data through network stream
                var netStream = GetStreamAsync(tryOnceOnly: true).Result;
                connected = true;
            }
            catch (Exception ex)
            {
                _log.ErrorFormat("Exception occured in Socket handler task.  Exception: {0}", ex);
            }

            return connected;
        }
        private void DedicatedSocketTask()
        {
            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    //block here until we can get connections then start loop pushing data through network stream
                    var netStream = GetStreamAsync().Result;

                    ProcessNetworkstreamTasks(netStream);
                }
                catch (Exception ex)
                {
                    if (_disposeToken.IsCancellationRequested)
                    {
                        _log.WarnFormat("KafkaTcpSocket thread shutting down because of a dispose call.");
                        var disposeException = new ObjectDisposedException("Object is disposing.");
                        _sendTaskQueue.DrainAndApply(t => t.Tcp.TrySetException(disposeException));
                        _readTaskQueue.DrainAndApply(t => t.Tcp.TrySetException(disposeException));
                        return;
                    }

                    if (ex is ServerDisconnectedException || ex.InnerException is ServerDisconnectedException)
                    {
                        if (OnServerDisconnected != null) OnServerDisconnected();
                        _log.ErrorFormat(ex.Message);
                        continue;
                    }

                    if (ex is ServerUnreachableException || ex.InnerException is ServerUnreachableException)
                    {
                        if (OnServerUnreachable != null) OnServerUnreachable(_endpoint);
                        _log.ErrorFormat(ex.Message);
                        continue;
                    }

                    _log.ErrorFormat("Exception occured in Socket handler task.  Exception: {0}", ex);
                }
            }
        }

        private void ProcessNetworkstreamTasks(Stream netStream)
        {
            Task writeTask = Task.FromResult(true);
            Task readTask = Task.FromResult(true);

            //reading/writing from network steam is not thread safe
            //Read and write operations can be performed simultaneously on an instance of the NetworkStream class without the need for synchronization. 
            //As long as there is one unique thread for the write operations and one unique thread for the read operations, there will be no cross-interference 
            //between read and write threads and no synchronization is required. 
            //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx
            while (_disposeToken.IsCancellationRequested == false && netStream != null)
            {
                Task sendDataReady = Task.WhenAll(writeTask, _sendTaskQueue.OnHasDataAvailable(_disposeToken.Token));
                Task readDataReady = Task.WhenAll(readTask, _readTaskQueue.OnHasDataAvailable(_disposeToken.Token));

                Task.WaitAny(sendDataReady, readDataReady);

                var exception = new[] { writeTask, readTask }
                    .Where(x => x.IsFaulted && x.Exception != null)
                    .SelectMany(x => x.Exception.InnerExceptions)
                    .FirstOrDefault();

                if (exception != null) throw exception;

                if (sendDataReady.IsCompleted) writeTask = ProcessSentTasksAsync(netStream, _sendTaskQueue.Pop());
                if (readDataReady.IsCompleted) readTask = ProcessReadTaskAsync(netStream, _readTaskQueue.Pop());
            }
        }

        private async Task ProcessReadTaskAsync(Stream netStream, SocketPayloadReadTask readTask)
        {
            using (readTask)
            {
                try
                {
                    StatisticsTracker.IncrementGauge(StatisticGauge.ActiveReadOperation);
                    var readSize = readTask.ReadSize;
                    var result = new List<byte>(readSize);
                    var bytesReceived = 0;

                    while (bytesReceived < readSize)
                    {
                        readSize = readSize - bytesReceived;

                        if (readSize >= KafkaOptions.MaxReadSize)
                        {
                            _log.FatalFormat("Too huge memory allocation {0}", readSize);
                            throw new ApplicationException(String.Format("Too huge memory allocation {0}", readSize));
                        }
                        var buffer = new byte[readSize];

                        if (OnReadFromSocketAttempt != null) OnReadFromSocketAttempt(readSize);

                        try
                        {
                            bytesReceived = await netStream.ReadAsync(buffer, 0, readSize, readTask.CancellationToken)
                                .WithCancellation(readTask.CancellationToken).ConfigureAwait(false);
                        }
                        catch (IOException ioException)
                        {
                            // this occurs every 5 mins in prod because the tcp connection is idle for too long time between the two uploads. 
                            // Exception occured in Socket handler task.  Exception: System.IO.IOException: Unable to read data from the transport connection: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond. ---> System.Net.Sockets.SocketException: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond
                            // at System.Net.Sockets.NetworkStream.EndRead(IAsyncResult asyncResult)
                            // --- End of inner exception stack trace ---
                            // at KafkaNet.KafkaTcpSocket.ProcessNetworkstreamTasks(Stream netStream)
                            // at KafkaNet.KafkaTcpSocket.DedicatedSocketTask()
                            using (_sslStream)
                            using (_client)
                            {
                                _client = null;
                                _sslStream = null;
                                throw new ServerDisconnectedException(string.Format("IOException occurs in read {0}", ioException.ToString()));
                            }
                        }

                        if (OnBytesReceived != null) OnBytesReceived(bytesReceived);

                        if (bytesReceived <= 0)
                        {
                            using (_sslStream)
                            using (_client)
                            {
                                _client = null;
                                _sslStream = null;
                                throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));
                            }
                        }

                        result.AddRange(buffer.Take(bytesReceived));
                    }

                    readTask.Tcp.TrySetResult(result.ToArray());
                }
                catch (Exception ex)
                {
                    if (_disposeToken.IsCancellationRequested)
                    {
                        var exception = new ObjectDisposedException("Object is disposing.");
                        readTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    if (ex is ServerDisconnectedException)
                    {
                        readTask.Tcp.TrySetException(ex);
                        throw;
                    }

                    //if an exception made us lose a connection throw disconnected exception
                    if (_client == null || _client.Connected == false)
                    {
                        var exception = new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));
                        readTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    readTask.Tcp.TrySetException(ex);
                    throw;
                }
                finally
                {
                    StatisticsTracker.DecrementGauge(StatisticGauge.ActiveReadOperation);
                }
            }
        }

        private async Task ProcessSentTasksAsync(Stream netStream, SocketPayloadSendTask sendTask)
        {
            if (sendTask == null) return;

            using (sendTask)
            {
                var failed = false;
                var sw = Stopwatch.StartNew();
                try
                {
                    sw.Restart();
                    StatisticsTracker.IncrementGauge(StatisticGauge.ActiveWriteOperation);

                    if (OnWriteToSocketAttempt != null) OnWriteToSocketAttempt(sendTask.Payload);
                    await sendTask.Payload.WriteToStream(netStream).ConfigureAwait(false);
                    sendTask.Tcp.TrySetResult(sendTask.Payload);
                }
                catch (Exception ex)
                {
                    failed = true;
                    if (_disposeToken.IsCancellationRequested)
                    {
                        var exception = new ObjectDisposedException("Object is disposing.");
                        sendTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    sendTask.Tcp.TrySetException(ex);
                    throw;
                }
                finally
                {
                    sendTask.Payload.Packer.Dispose();
                    StatisticsTracker.DecrementGauge(StatisticGauge.ActiveWriteOperation);
                    StatisticsTracker.CompleteNetworkWrite(sendTask.Payload, sw.ElapsedMilliseconds, failed);
                }
            }
        }

        private async Task<Stream> GetStreamAsync(bool tryOnceOnly = false)
        {
            //using a semaphore here to allow async waiting rather than blocking locks
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false))
            {
                if ((_client == null || _client.Connected == false) && !_disposeToken.IsCancellationRequested)
                {
                    _client = await ReEstablishConnectionAsync(tryOnceOnly).ConfigureAwait(false);
                }

                if (!this._enableSSL)
                {
                    return _client == null ? null : _client.GetStream();
                }
                else
                {
                    return _client == null ? null : await this.GetSslStream();
                }
            }
        }

        private async Task<SslStream> GetSslStream()
        {
            if (_sslStream != null)
            {
                return _sslStream;
            }

            // Create an SSL stream that will close the client's stream.
            SslStream sslStream = new SslStream(
                _client.GetStream(),
                false,
                new RemoteCertificateValidationCallback(this.ValidateServerCertificate),
                null
                );
            // The server name must match the name on the server certificate. 

            bool authException = false;
            try
            {
                X509Certificate2Collection clientCertificates = this.GetClientCert();
                var task = sslStream.AuthenticateAsClientAsync(_endpoint.ServeUri.Host, clientCertificates, System.Security.Authentication.SslProtocols.Tls12, false);
                bool finished = task.Wait(TimeSpan.FromMinutes(3));
                if(!finished)
                {
                    string s = "Timeout. Closing the connection and try again";
                    _log.ErrorFormat(s);
                    _client.Close();
                    _client = null;
                    throw new ServerUnreachableException(s);
                }
            }
            catch (AuthenticationException e)
            {
                authException = true;
                _log.FatalFormat("Exception: {0}", e.Message);
                if (e.InnerException != null)
                {
                    _log.FatalFormat("Inner exception: {0}", e.InnerException.Message);
                }
                _log.ErrorFormat("Authentication failed - closing the connection.");
                _client.Close();
                sslStream.Close();
                sslStream.Dispose();
            }

            if (authException)
            {
                X509Certificate2Collection nextClientCertificate = this.GetNextClientCert();

                if (nextClientCertificate != null)
                {
                    _log.InfoFormat("Authentication failed with current certificate, try next client certificate");

                    _client = await ReEstablishConnectionAsync().ConfigureAwait(false);

                    sslStream = new SslStream(
                                                _client.GetStream(),
                                                false,
                                                new RemoteCertificateValidationCallback(this.ValidateServerCertificate),
                                                null
                                                );
                    try
                    {
                        sslStream.AuthenticateAsClient(_endpoint.ServeUri.Host, nextClientCertificate, SslProtocols.Tls12, false);
                        clientCerts = nextClientCertificate;
                    }
                    catch (AuthenticationException e2)
                    {
                        if (e2.InnerException != null)
                        {
                            _log.FatalFormat("Inner exception: {0}", e2.InnerException.Message);
                        }

                        _log.FatalFormat("Authentication failed with next client certificate - closing the connection.");
                        sslStream.Close();
                        sslStream.Dispose();
                        _client.Close();
                        return null;
                    }
                }
                else
                {
                    _client.Close();
                    return null;
                }
            }

            _sslStream = sslStream;
            return sslStream;
        }

        private X509Certificate2Collection GetClientCert()
        {
            if (KafkaTcpSocket.clientCerts == null)
            {
                KafkaTcpSocket.clientCerts = this.FindCertificatesByThumbprint(this._privateCertThumbprint);
            }

            return KafkaTcpSocket.clientCerts;
        }

        private X509Certificate2Collection GetNextClientCert()
        {
            if (KafkaTcpSocket.nextClientCerts == null)
            {
                KafkaTcpSocket.nextClientCerts = this.FindCertificatesByThumbprint(this._nextPrivateCertThumbprint);
            }

            return KafkaTcpSocket.nextClientCerts;
        }

        private X509Certificate2Collection FindCertificatesByThumbprint(StoreLocation location, string thumprint)
        {
            X509Certificate2Collection certificates = null;

            var store = new X509Store(location);
            try
            {
                store.Open(OpenFlags.ReadOnly);
                certificates = store.Certificates.Find(X509FindType.FindByThumbprint, thumprint, false);
            }
            catch (System.Security.SecurityException)
            {
            }
            finally
            {
                store.Close();
            }

            return certificates;
        }

        private X509Certificate2Collection FindCertificatesByThumbprint(string thumprint)
        {
            var certificates = this.FindCertificatesByThumbprint(StoreLocation.LocalMachine, thumprint);
            if (certificates == null || certificates.Count == 0)
            {
                certificates = this.FindCertificatesByThumbprint(StoreLocation.CurrentUser, thumprint);
            }

            return certificates;
        }

        // The following method is invoked by the RemoteCertificateValidationDelegate. 
        public bool ValidateServerCertificate(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            _log.ErrorFormat("Certificate error: {0}", sslPolicyErrors);

            // TODO: BUGBUG. Do allow this client to communicate with unauthenticated servers for now.
            return true;
        }

        /// <summary>
        /// (Re-)establish the Kafka server connection.
        /// Assumes that the caller has already obtained the <c>_clientLock</c>
        /// </summary>
        private async Task<TcpClient> ReEstablishConnectionAsync(bool tryOnceOnly = false)
        {
            var attempts = 1;
            const int max_attempts = 5;
            var reconnectionDelay = DefaultReconnectionTimeout;
            _log.WarnFormat("No connection to:{0}.  Attempting to connect...", _endpoint);

            _client = null;
            if (_sslStream != null)
            {
                _sslStream.Close();
                _sslStream.Dispose();
                _sslStream = null;
            }

            while (_disposeToken.IsCancellationRequested == false && attempts < max_attempts)
            {
                try
                {
                    attempts++;
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_endpoint.Endpoint.Address, _endpoint.Endpoint.Port).ConfigureAwait(false);
                    _log.WarnFormat("Connection established to:{0}.", _endpoint);
                    return _client;
                }
                catch (Exception e)
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    reconnectionDelay = Math.Min(reconnectionDelay, (int)_maximumReconnectionTimeout.TotalMilliseconds);

                    if (tryOnceOnly)
                    {
                        throw;
                    }
                    else
                    {
                        _log.WarnFormat("Failed connection to:{0}. Exception {1}.  Will retry in:{2}.", _endpoint, e.ToString(), reconnectionDelay);
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token).ConfigureAwait(false);
            }

            if (!_client.Connected && attempts >= max_attempts)
            {
                throw new ServerUnreachableException("unable to connect {0} after {1} tries", _endpoint, attempts);
            }
            return _client;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            if (_disposeToken != null) _disposeToken.Cancel();

            using (_disposeToken)
            using (_disposeRegistration)
            using (_client)
            using (_sslStream)
            { }

            if (_socketTask.Status != TaskStatus.Running) // when running, it cannot be disposed. According to MSDN (http://blogs.msdn.com/b/pfxteam/archive/2012/03/25/10287435.aspx), there's no need to dispose Task.
            {
                using (_socketTask)
                {
                    _socketTask.SafeWait(TimeSpan.FromSeconds(30));
                }
            }
        }
    }

    class SocketPayloadReadTask : IDisposable
    {
        public CancellationToken CancellationToken { get; private set; }
        public TaskCompletionSource<byte[]> Tcp { get; set; }
        public int ReadSize { get; set; }

        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        public SocketPayloadReadTask(int readSize, CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
            Tcp = new TaskCompletionSource<byte[]>();
            ReadSize = readSize;
            _cancellationTokenRegistration = cancellationToken.Register(() => Tcp.TrySetCanceled());
        }

        public void Dispose()
        {
            using (_cancellationTokenRegistration)
            {

            }
        }
    }

    class SocketPayloadSendTask : IDisposable
    {
        public TaskCompletionSource<KafkaDataPayload> Tcp { get; set; }
        public KafkaDataPayload Payload { get; set; }

        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        public SocketPayloadSendTask(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            Tcp = new TaskCompletionSource<KafkaDataPayload>();
            Payload = payload;
            _cancellationTokenRegistration = cancellationToken.Register(() => Tcp.TrySetCanceled());
        }

        public void Dispose()
        {
            using (_cancellationTokenRegistration)
            {

            }
        }
    }

    public class KafkaDataPayload
    {
        public int CorrelationId { get; set; }
        public ApiKeyRequestType ApiKey { get; set; }
        public int MessageCount { get; set; }
        public bool TrackPayload
        {
            get { return MessageCount > 0; }
        }

        public KafkaMessagePooledPacker Packer
        {
            get;
            set;
        }

        public PayloadFlag OutputFlag
        {
            get;
            set;
        }

        public async Task WriteToStream(Stream stream)
        {
            if (OutputFlag == PayloadFlag.Payload)
            {
                await Packer.WritePayloadAsync(stream);
            }
            else if (OutputFlag == PayloadFlag.PayloadNoLength)
            {
                await Packer.WritePayloadNotLengthAsync(stream);
            }
            else if (OutputFlag == PayloadFlag.CRC)
            {
                await Packer.WriteCrcPayloadAsync(stream);
            }
        }
    }

    public enum PayloadFlag
    {
        Payload,
        PayloadNoLength,
        CRC
    }
}
