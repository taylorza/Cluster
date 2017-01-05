using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    [Serializable]
    public class ClusterNode : IEquatable<ClusterNode>
    {
        [NonSerialized]
        private DateTime _lastSeen;

        [NonSerialized]
        private int _errorCount;

        internal long Epoch { get; private set; }
        public int HeartBeat { get; private set; } = 1;
        public IPEndPoint EndPoint { get; private set; }

        public DateTime LastSeen
        {
            get { return _lastSeen; }
            private set { _lastSeen = value; }
        }

        public int ErrorCount
        {
            get { return _errorCount; }
            internal set { _errorCount = value; }
        }

        public bool IsShuttingDown { get; set; }

        internal ClusterNode(ClusterNode node)
        {
            Epoch = node.Epoch;
            HeartBeat = node.HeartBeat;            
            EndPoint = node.EndPoint;
            LastSeen = DateTime.Now;
        }

        internal ClusterNode(IPEndPoint endpoint, bool isOriginNode)
        {
            Epoch = (DateTime.UtcNow - new DateTime(1970, 1, 1)).Ticks / TimeSpan.TicksPerSecond;
            HeartBeat = 1;            
            EndPoint = endpoint;
            LastSeen = DateTime.Now;
        }

        internal void Update()
        {
            Update(HeartBeat + 1, 0);
        }

        internal void Update(int heartBeat, int errorCount = 0)
        {
            HeartBeat = heartBeat;
            LastSeen = DateTime.Now;
            ErrorCount = errorCount;
        }

        internal void Synchronize(ClusterNode node)
        {
            Epoch = node.Epoch;
            Update(node.HeartBeat);
        }

        public bool Equals(ClusterNode other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return IPEndPoint.Equals(this.EndPoint, other.EndPoint);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ClusterNode);
        }

        public override int GetHashCode()
        {
            return EndPoint.GetHashCode();
        }
    }
}
