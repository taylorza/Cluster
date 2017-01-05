using Cluster.Messages;
using System;
using System.Collections.Concurrent;
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

        private Timer _gossipTimer;
        private Timer _scavangeTimer;

        private Random _random = new Random();
        private IndexedList<ClusterNode> _activeNodes = new IndexedList<ClusterNode>();
        private IndexedList<ClusterNode> _deadNodes = new IndexedList<ClusterNode>();
        private BlockingCollection<IMessage> _gossipQueue = new BlockingCollection<IMessage>();
        private ConcurrentDictionary<Guid, IMessage> _previousMessages = new ConcurrentDictionary<Guid, IMessage>();

        private ClusterNode[] _seedNodes;
        private ClusterNode _localNode;
        private volatile bool _startShutdown;
        private CancellationTokenSource _shutdownCancellationTokenSource;
        private State _currentState;

        internal ClusterManager(ClusterServer clusterServer, ClusterNode localNode, IEnumerable<ClusterNode> seedNodes)
        {
            _clusterServer = clusterServer;
            _seedNodes = seedNodes.ToArray();
            _localNode = localNode;
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

            _activeNodes.Add(_localNode);
        }

        internal async Task Start()
        {
            _currentState = State.JoinCluster;

            _startShutdown = false;
            _shutdownCancellationTokenSource = new CancellationTokenSource();

            var seedNodes = SelectGossipPartners();
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
                StartBackgroudProcessing();
            }
        }

        private void StartBackgroudProcessing()
        {
            _gossipTimer = new Timer(GossipActiveNodes, null, _gossipPeriod, _gossipPeriod);
            _scavangeTimer = new Timer(ScavangeActiveNodes, null, _scavangePeriod, _scavangePeriod);
            var task = Task.Factory.StartNew(ProcessGossipQueue, TaskCreationOptions.LongRunning, _shutdownCancellationTokenSource.Token);
        }

        private void StopBackgroundProcessing()
        {
            _gossipTimer.Change(Timeout.Infinite, Timeout.Infinite);
            _scavangeTimer.Change(Timeout.Infinite, Timeout.Infinite);
            _gossipTimer.Dispose();
            _scavangeTimer.Dispose();

            _gossipQueue.CompleteAdding();
            _shutdownCancellationTokenSource.Cancel();
            
            Thread.Sleep(_gossipPeriod);
        }

        internal void Shutdown()
        {
            _startShutdown = true;
            StopBackgroundProcessing();

            var index = _activeNodes.IndexOf(_localNode);
            if (index >= 0)
            {
                var node = _activeNodes[index];
                node.Update(int.MaxValue - 2);
                node.IsShuttingDown = true;
                GossipMessageImmediately(new LeaveClusterNotification(_clusterId, node));
            }            
            
            Thread.Sleep(_gossipPeriod);
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
            if (_startShutdown) return;

            bool dispatchMessage = true;
            message.LastSeen = DateTime.Now;

            // If the message has a time to live limit, it is checked for duplication and will be re-gossiped 
            // upto TimeToLive times from this cluster node instance
            if (message.TimeToLive > 0)
            {
                IMessage previousMessage;
                if (_previousMessages.TryGetValue(message.MessageId, out previousMessage))
                {
                    previousMessage.LastSeen = message.LastSeen;
                    previousMessage.DuplicateCount++;
                    message.DuplicateCount = previousMessage.DuplicateCount;
                }
                else
                {
                    _previousMessages.AddOrUpdate(message.MessageId, message, (g, m) => m);
                }

                if (message.DuplicateCount > message.TimeToLive)
                {
                    dispatchMessage = false;
                }
                else
                {
                    GossipMessage(message);
                }
            }

            if (message.DuplicateCount > 0 && message.IgnoreIfDuplicate)
            {
                dispatchMessage = false;
            }

            if (dispatchMessage)
            {
                if (message is ShareClusterNodes)
                {
                    var m = message as ShareClusterNodes;
                    await Task.Run(() => ShareClusterNodesHandler(m));
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
                else if (message is LeaveClusterNotification)
                {
                    var m = message as LeaveClusterNotification;
                    LeaveClusterNotificationHandler(m);
                }
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

        private void GossipMessage(IMessage message)
        {
            _gossipQueue.Add(message);
        }

        private void GossipMessageImmediately(IMessage message)
        {
            ParallelOptions po = new ParallelOptions();
            po.MaxDegreeOfParallelism = 4;

            var payload = CreatePayload(message);
            var gossipPartners = SelectGossipPartners();
            try
            {
                Parallel.ForEach(gossipPartners, po,
                    async (node, loopState) =>
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
            catch (OperationCanceledException)
            {
                // Shutdown requested
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
                StartBackgroudProcessing();
                //TODO: Raise event here indicating that the cluster was successfully joined
            }
            else
            {
                Shutdown();
                //TODO: Raise event here indicating the failure to join the cluster
            }
        }

        private void ShareClusterNodesHandler(ShareClusterNodes message)
        {
            UpdateNodes(message.Nodes);
        }

        private void LeaveClusterNotificationHandler(LeaveClusterNotification message)
        {
            lock(_activeNodes)
            {
                int index = _activeNodes.IndexOf(message.Node);
                if (index >= 0)
                {
                    var node = _activeNodes[index];                    
                    node.Update(message.Node.HeartBeat);
                    node.IsShuttingDown = true;
                }
            }
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
                            if ((activeNode.Epoch == remoteNode.Epoch && activeNode.HeartBeat < remoteNode.HeartBeat) 
                                || activeNode.Epoch < remoteNode.Epoch)
                            {
                                activeNode.Synchronize(remoteNode);
                                activeNode.IsShuttingDown = remoteNode.IsShuttingDown;
                            }
                        }
                        else
                        {
                            index = _deadNodes.IndexOf(remoteNode);
                            if (index >= 0)
                            {
                                var deadNode = _deadNodes[index];
                                if ((deadNode.Epoch == remoteNode.Epoch && deadNode.HeartBeat < remoteNode.HeartBeat)
                                    || deadNode.Epoch < remoteNode.Epoch)
                                {
                                    _deadNodes.RemoveAt(index);
                                    _activeNodes.Add(deadNode);
                                    deadNode.Synchronize(remoteNode);
                                }
                            }
                            else if (!remoteNode.IsShuttingDown)
                            {
                                ClusterNode newNode = new ClusterNode(remoteNode);
                                _activeNodes.Add(newNode);
                            }
                        }
                    }
                }
            }
        }

        private void ProcessGossipQueue(object state)
        {
            ParallelOptions po = new ParallelOptions();
            po.CancellationToken = _shutdownCancellationTokenSource.Token;
            
            try
            {
                IMessage message;
                while (!_shutdownCancellationTokenSource.Token.IsCancellationRequested)
                {
                    if (_gossipQueue.TryTake(out message, -1, _shutdownCancellationTokenSource.Token))
                    {
                        GossipMessageImmediately(message);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Shutdown was requested               
            }            
        }

        private void GossipActiveNodes(object state)
        {
            _localNode.Update();

            var nodes = CloneActiveNodes();
            GossipMessage(new ShareClusterNodes(nodes));
        }

        private void ScavangeActiveNodes(object state)
        {
            // TODO: Scavange _previousMessages

            lock (_activeNodes)
            {
                for (int i = _activeNodes.Count - 1; i >= 0; --i)
                {
                    var node = _activeNodes[i];
                    if (node.Equals(_localNode)) continue;

                    var timeSpan = (DateTime.Now - node.LastSeen).TotalMilliseconds;
                    if (timeSpan > _nodeExpiryPeriod 
                        || node.ErrorCount > _maxFailedGossipAttempts)
                    {
                        _activeNodes.RemoveAt(i);
                        if (!node.IsShuttingDown)
                        {
                            lock (_deadNodes)
                            {
                                node.ErrorCount = 0;
                                _deadNodes.Add(node);
                            }
                        }
                    }
                }
            }
        }

        private ClusterNode[] SelectGossipPartners()
        {
            var sourceNodes = CloneActiveNodes().Concat(_seedNodes).Distinct().ToArray();
            Shuffle(sourceNodes);

            ClusterNode[] nodes = { };
            if (_random.NextDouble() < 0.1)
            {
                var deadnodes = CloneDeadNodes();
                if (deadnodes.Length > 0)
                {
                    nodes = new ClusterNode[] { deadnodes[_random.Next(deadnodes.Length)] };
                }
            }
            return nodes.Concat(sourceNodes.Where((node) => !node.Equals(_localNode) && !node.IsShuttingDown)).Take(_gossipSpan).ToArray();
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
