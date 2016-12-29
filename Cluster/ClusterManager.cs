using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cluster
{
    class ClusterManager
    {
        private int _gossipPeriod = 1000;
        private int _scavangePeriod = 10000;

        private Random _random = new Random();
        private IndexedList<ClusterNode> _activeNodes = new IndexedList<ClusterNode>();
        private IndexedList<ClusterNode> _deadNodes = new IndexedList<ClusterNode>();

        private ClusterNode _localNode;
        private CancellationTokenSource _shutdownCancellationTokenSource;

        public ClusterManager(ClusterNode localNode, IEnumerable<ClusterNode> seedNodes)
        {
            _localNode = localNode;
            _activeNodes.Add(_localNode);
            foreach(var node in seedNodes)
            {
                _activeNodes.Add(node);
            }
        }

        public void Start()
        {
            _shutdownCancellationTokenSource = new CancellationTokenSource();

            Task.Factory.StartNew(async (_) => {  await GossipTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
            Task.Factory.StartNew(async (_) => { await ScavangeTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
        }

        public void Shutdown()
        {
            _shutdownCancellationTokenSource.Cancel();            
        }

        public void UpdateNodes(IEnumerable<ClusterNode> remoteNodes)
        {
            lock(_activeNodes)
            {
                lock(_deadNodes)
                {
                    foreach(var remoteNode in remoteNodes)
                    {
                        var index = _activeNodes.IndexOf(remoteNode);
                        if (index >= 0)
                        {
                            var activeNode = _activeNodes[index];
                            if (activeNode.HeartBeat < remoteNode.HeartBeat)
                            {
                                activeNode.Update(remoteNode.HeartBeat);
                            }
                        }
                        else
                        {
                            index = _deadNodes.IndexOf(remoteNode);
                            if (index >= 0)
                            {
                                var deadNode = _deadNodes[index];
                                if (deadNode.HeartBeat < remoteNode.HeartBeat || remoteNode.IsOriginNode)
                                {
                                    _deadNodes.RemoveAt(index);
                                    _activeNodes.Add(deadNode);
                                    deadNode.Update(remoteNode.HeartBeat);
                                }                                
                            }
                            else
                            {
                                ClusterNode newNode = new ClusterNode(remoteNode.EndPoint, false);
                                newNode.Update(remoteNode.HeartBeat);
                                _activeNodes.Add(newNode);                                
                            }
                        }
                    }
                }
            }
        }

        // TODO: Create clones of the Active and Dead list which contain deep clones of the ClusterNodes
        public List<ClusterNode> GetActiveNodes()
        {
            lock (_activeNodes)
            {
                return _activeNodes.ToList();
            }
        }

        // TODO: Create clones of the Active and Dead list which contain deep clones of the ClusterNodes
        public List<ClusterNode> GetDeadNodes()
        {
            lock (_deadNodes)
            {
                return _deadNodes.ToList();
            }
        }

        private async Task GossipTask()
        {
            try
            {
                while (!_shutdownCancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(_gossipPeriod, _shutdownCancellationTokenSource.Token);
                    GossipActiveNodes();
                }
            }
            catch(TaskCanceledException)
            {
                // Delay was canceled
            }
        }

        private async Task ScavangeTask()
        {
            try
            {
                while (!_shutdownCancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(_scavangePeriod, _shutdownCancellationTokenSource.Token);
                    ScavangeActiveNodes();
                }
            }
            catch (TaskCanceledException)
            {
                // Delay was canceled
            }
        }

        private void ScavangeActiveNodes()
        {
            lock (_activeNodes)
            {
                for (int i = _activeNodes.Count - 1; i >= 0; --i)
                {
                    var node = _activeNodes[i];
                    if (node == _localNode) continue;

                    if ((DateTime.Now - node.LastSeen).TotalMilliseconds > this._scavangePeriod || node.ErrorCount > 3)
                    {
                        _activeNodes.RemoveAt(i);
                        lock (_deadNodes)
                        {
                            _deadNodes.Add(node);
                        }
                    }
                }
            }
        }

        private void GossipActiveNodes()
        {
            _localNode.Update();

            var nodes = CloneActiveNodes();

            ClusterNode[] gossipPartners = null;
            if (_deadNodes.Count > 0 && _random.NextDouble() < 0.1)
            {
                gossipPartners = SelectGossipPartners(CloneDeadNodes());
            }
            else
            {
                gossipPartners = SelectGossipPartners(nodes);
                if (gossipPartners.Length == 0)
                {
                    gossipPartners = SelectGossipPartners(CloneDeadNodes());
                }
            }

            byte[] message = null;
            using (var ms = new MemoryStream())
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(ms, nodes);
                message = MessageProtocol.PackMessage(ms.ToArray());
            }

            Parallel.ForEach(gossipPartners,
                async (node) =>
                {
                    try
                    {
                        using (var tcpClient = new TcpClient())
                        {
                            await tcpClient.ConnectAsync(node.EndPoint.Address, node.EndPoint.Port);
                            var stream = tcpClient.GetStream();
                            using (var compressionStream = new DeflateStream(stream, CompressionMode.Compress, true))
                            {
                                await compressionStream.WriteAsync(message, 0, message.Length);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        node.ErrorCount++;
                        Debug.WriteLine(ex);
                    }                                           
                });            
        }

        private ClusterNode[] SelectGossipPartners(ClusterNode[] sourceNodes)
        {
            Shuffle(sourceNodes);
            return sourceNodes.Where((node) => node != _localNode).Take(Math.Min((sourceNodes.Length / 2) + 1, 3)).ToArray();
        }

        private ClusterNode[] CloneActiveNodes()
        {
            lock (_activeNodes)
            {
                return _activeNodes.ToArray();
            }
        }

        private ClusterNode[] CloneDeadNodes()
        {
            lock(_deadNodes)
            {
                return _deadNodes.ToArray();
            }
        }

        private void Shuffle(ClusterNode[] nodes)
        {
            for(int i = 0; i < nodes.Length - 1; ++i)
            {
                int j = _random.Next(i, nodes.Length);
                if (j == i) continue;
                ClusterNode t = nodes[i];
                nodes[i] = nodes[j];
                nodes[j] = t;
            }
        }
    }
}
