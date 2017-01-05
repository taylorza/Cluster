using Cluster.Messages;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    class ClusterServer
    {
        private const int BUFFER_SIZE = 4096;

        private TcpListener _listener;
        private ClusterManager _manager;
        private ClusterNodeState _localNode;
        private int _serverPort;
        private volatile bool _running;

        public ClusterServer(int serverPort)
        {
            _serverPort = serverPort;
            _localNode = new ClusterNodeState(new IPEndPoint(NetworkUtil.GetLocalIPAddress(), _serverPort), true);

            var seedNodes = new List<ClusterNodeState>();
            var seedNodesString = ConfigurationManager.AppSettings.Get("seedNodes");            
            if (!string.IsNullOrWhiteSpace(seedNodesString))
            {
                foreach(var seedNode in seedNodesString.Split(','))
                {
                    IPEndPoint ep = NetworkUtil.CreateIPEndPoint(seedNode);
                    if (!IPEndPoint.Equals(ep, _localNode.EndPoint)) seedNodes.Add(new ClusterNodeState(ep, false));
                }
            }
            _manager = new ClusterManager(this, _localNode, seedNodes);
        }

        public async Task Start()
        {
            _running = true;
            _listener = new TcpListener(IPAddress.Any, _serverPort);
            _listener.Start();
            
            await _manager.Start();

            while(_running)
            {
                try
                {
                    var tcpClient = await _listener.AcceptTcpClientAsync();
                    var catchAll = HandleClientAsync(tcpClient);
                }
                catch(ObjectDisposedException)
                {
                    Debug.WriteLine("AcceptTcpClientAsync cancelled");
                }
            }
        }

        public void Shutdown()
        {
            if (_running)
            {
                _running = false;
                _manager.Shutdown();
                if (_listener != null) _listener.Stop();                
            }
        }

        public List<ClusterNodeState> GetDeadNodes()
        {
            return _manager.GetDeadNodes();
        }

        public List<ClusterNodeState> GetActiveNodes()
        {
            return _manager.GetActiveNodes();
        }

        private async Task HandleClientAsync(TcpClient tcpClient)
        {
            using (tcpClient)
            {
                try
                {
                    MessageProtocol protocol = new MessageProtocol();
                    protocol.MessageArrived += HandleMessage;

                    byte[] buffer = new byte[BUFFER_SIZE];

                    var stream = tcpClient.GetStream();
                    using (var compressionStream = new DeflateStream(stream, CompressionMode.Decompress, true))
                    {
                        while (true)
                        {
                            try
                            {
                                var bytesRead = await compressionStream.ReadAsync(buffer, 0, buffer.Length);
                                if (bytesRead <= 0)
                                {
                                    break;
                                }
                                protocol.DataReceived(buffer, 0, bytesRead);
                            }
                            catch (SocketException ex)
                            {
                                Debug.WriteLine(ex);
                                break;
                            }
                            catch (Exception ex)
                            {
                                Debug.WriteLine(ex);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }
            }
        }

        private void HandleMessage(object sender, MessageEventArgs e)
        {
            MemoryStream ms = new MemoryStream(e.Message);
            BinaryFormatter bf = new BinaryFormatter();
            var o = bf.Deserialize(ms);
            var handled = _manager.DispatchMessage(o as IMessage);            
        }
    }
}
