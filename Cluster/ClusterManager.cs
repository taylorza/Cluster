using Cluster.Messages;
using System;
using System.Collections.Generic;
using System.Configuration;
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
        private ClusterServer _clusterServer;

        private Guid _leaderElectionId;
        private ClusterNode _leader;
        private string _clusterId;
        private int _gossipSpan;
        private int _gossipPeriod;
        private int _scavangePeriod;
        private int _nodeExpiryPeriod;
        private int _maxFailedGossipAttempts;

        private Random _random = new Random();
        private IndexedList<ClusterNode> _activeNodes = new IndexedList<ClusterNode>();
        private IndexedList<ClusterNode> _deadNodes = new IndexedList<ClusterNode>();

        private ClusterNode _localNode;
        private CancellationTokenSource _shutdownCancellationTokenSource;
        private State _currentState;
        
        internal ClusterManager(ClusterServer clusterServer, ClusterNode localNode, IEnumerable<ClusterNode> seedNodes)
        {
            _clusterServer = clusterServer;
            _leaderElectionId = Guid.NewGuid();

            _clusterId = ConfigurationManager.AppSettings["clusterId"];

            //TODO: Create a custom configuration section to manage all the config data
            //TODO: Add validation of the configuration values
            if (!int.TryParse(ConfigurationManager.AppSettings["gossipPeriod"], out _gossipPeriod))
            {                
                _gossipPeriod = 1000;
                Trace.TraceInformation($"Failed to read <gossipPeriod> from config, defaulting to {_gossipPeriod}ms");
            }

            if (!int.TryParse(ConfigurationManager.AppSettings["gossipSpan"], out _gossipSpan))
            {
                _gossipSpan = 3;
                Trace.TraceInformation($"Failed to read <scavangePeriod> from config, defaulting to {_gossipSpan}ms");
            }

            if (!int.TryParse(ConfigurationManager.AppSettings["scavangePeriod"], out _scavangePeriod))
            {
                _scavangePeriod = _gossipPeriod * 10;
                Trace.TraceInformation($"Failed to read <scavangePeriod> from config, defaulting to {_scavangePeriod}ms");
            }

            if (!int.TryParse(ConfigurationManager.AppSettings["nodeExpiryPeriod"], out _nodeExpiryPeriod))
            {
                _nodeExpiryPeriod = Math.Max(_scavangePeriod, _gossipPeriod * 10);
                Trace.TraceInformation($"Failed to read <nodeExpiryPeriod> from config, defaulting to {_nodeExpiryPeriod}ms");
            }
            
            if (!int.TryParse(ConfigurationManager.AppSettings["maxFailedGossipAttempts"], out _maxFailedGossipAttempts))
            {
                _maxFailedGossipAttempts = 3;
                Trace.TraceInformation($"Failed to read <maxFailedGossipAttempts> from config, defaulting to {_maxFailedGossipAttempts}");
            }

            _localNode = localNode;
            _activeNodes.Add(_localNode);
            foreach(var node in seedNodes)
            {
                _activeNodes.Add(node);
            }
        }

        internal async Task Start()
        {
            _currentState = State.JoinCluster;

            _shutdownCancellationTokenSource = new CancellationTokenSource();

            var seedNodes = SelectGossipPartners(CloneActiveNodes());
            bool connectedToSeed = false;
            foreach (var node in seedNodes)
            {
                try
                {
                    await SendMessage(node, new JoinClusterRequest(_clusterId, _localNode));
                    connectedToSeed = true;
                    break;
                }
                catch (Exception ex)
                {
                    Trace.TraceInformation($"Could not contact node : {ex.Message}");
                }
            }
            
            // TODO: Refactor the handling of the startup handshake - Pending refactor of Cluster Manager as a state machine?
            // 1. It should run if it was not posible to connect to any of the seeds (what it does now)
            // 2. If a seed was contacted but no response was returned within a configured timeout period this should run. The seed process contacted might have failed.
            // 3. If a seed was contacted but it rejected the request to join then this should not run
            //
            // 2 and 3 are not currently catered to, I think Cluster Manager needs to be refactored to run as a big state machine to simplify these and other
            // scenarios...
            if (!connectedToSeed)
            {
                Task task;
                task = Task.Factory.StartNew(async (_) => { await GossipTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
                task = Task.Factory.StartNew(async (_) => { await ScavangeTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
            }
        }

        internal void Shutdown()
        {
            _shutdownCancellationTokenSource.Cancel();            
        }

        // TODO: Create clones of the Active and Dead list which contain deep clones of the ClusterNodes
        internal List<ClusterNode> GetActiveNodes()
        {
            lock (_activeNodes)
            {
                return _activeNodes.ToList();
            }
        }

        // TODO: Create clones of the Active and Dead list which contain deep clones of the ClusterNodes
        internal List<ClusterNode> GetDeadNodes()
        {
            lock (_deadNodes)
            {
                return _deadNodes.ToList();
            }
        }

        enum State
        {
            JoinCluster,
            JoinedCluster,           
        }

        internal async Task DispatchMessage(IMessage message)
        {
            if (message is ShareClusterNodes)
            {
                var m = message as ShareClusterNodes;
                await Task.Run(()=>ShareClusterNodesHandler(m));
            }
            else if (message is JoinClusterRequest)
            {
                var m = message as JoinClusterRequest;
                var handled = JoinClusterRequestHandler(m);
            }
            else if (message is JoinClusterResponse)
            {
                var m = message as JoinClusterResponse;
                JoinClusterResponseHandler(m);
            }
        }

        private byte[] CreatePayload(IMessage message)
        {
            byte[] payload = null;
            using (var ms = new MemoryStream())
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(ms, message);
                payload = MessageProtocol.PackMessage(ms.ToArray());
            }
            return payload;
        }

        private async Task SendMessage(ClusterNode node, IMessage message)
        {
            var payload = CreatePayload(message);
            await SendMessage(node, payload);
        }

        private async Task SendMessage(ClusterNode node, byte[] payload)
        {
            using (var tcpClient = new TcpClient())
            {
                await tcpClient.ConnectAsync(node.EndPoint.Address, node.EndPoint.Port);

                var stream = tcpClient.GetStream();
                using (var compressionStream = new DeflateStream(stream, CompressionMode.Compress, true))
                {
                    await compressionStream.WriteAsync(payload, 0, payload.Length);
                }
            }
        }

        private async Task JoinClusterRequestHandler(JoinClusterRequest message)
        {
            if (!string.IsNullOrEmpty(_clusterId) && message.ClusterId != _clusterId)
            {
                await SendMessage(message.Node, new JoinClusterResponse(false, null));
            }
            else
            {
                await SendMessage(message.Node, new JoinClusterResponse(true, CloneActiveNodes()));
                UpdateNodes(message.Node);                
            }
        }

        private void JoinClusterResponseHandler(JoinClusterResponse message)
        {
            if (message.JoinSucceeded)
            {
                UpdateNodes(message.Nodes);
                Task.Factory.StartNew(async (_) => { await GossipTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
                Task.Factory.StartNew(async (_) => { await ScavangeTask(); }, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
            }
            else
            {
                throw new Exception("Request to join cluster rejected");
            }
        }

        private void ShareClusterNodesHandler(ShareClusterNodes message)
        {
            UpdateNodes(message.Nodes);
        }

        private void UpdateNodes(params ClusterNode[] remoteNodes)
        {
            lock (_activeNodes)
            {
                lock (_deadNodes)
                {
                    foreach (var remoteNode in remoteNodes)
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

        private async Task GossipTask()
        {
            try
            {
                while (!_shutdownCancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(_gossipPeriod, _shutdownCancellationTokenSource.Token);
                    try
                    {
                        GossipActiveNodes();
                    }
                    catch(Exception ex)
                    {
                        Debug.WriteLine(ex);
                    }
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

            var payload = CreatePayload(new ShareClusterNodes(nodes));
            
            Parallel.ForEach(gossipPartners,
                async (node) =>
                {
                    try
                    {
                        await SendMessage(node, payload);
                    }
                    catch (Exception ex)
                    {
                        node.ErrorCount++;
                        Trace.TraceInformation($"Gossip failed : {ex.Message}");
                    }
                });
        }

        private void ScavangeActiveNodes()
        {
            lock (_activeNodes)
            {
                for (int i = _activeNodes.Count - 1; i >= 0; --i)
                {
                    var node = _activeNodes[i];
                    if (node == _localNode) continue;

                    if ((DateTime.Now - node.LastSeen).TotalMilliseconds > _nodeExpiryPeriod 
                        || node.ErrorCount > _maxFailedGossipAttempts)
                    {
                        _activeNodes.RemoveAt(i);
                        lock (_deadNodes)
                        {
                            node.ErrorCount = 0;
                            _deadNodes.Add(node);
                        }
                    }
                }
            }
        }

        private ClusterNode[] SelectGossipPartners(ClusterNode[] sourceNodes)
        {
            Shuffle(sourceNodes);
            return sourceNodes.Where((node) => node != _localNode && node.ErrorCount <= _maxFailedGossipAttempts).Take(_gossipSpan).ToArray();
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
