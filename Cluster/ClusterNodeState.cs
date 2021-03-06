﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    [Serializable]
    public class ClusterNodeState : IEquatable<ClusterNodeState>
    {
        [NonSerialized]
        private object _syncRoot = new object();

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

        internal ClusterNodeState(ClusterNodeState node)
        {
            Epoch = node.Epoch;
            HeartBeat = node.HeartBeat;            
            EndPoint = node.EndPoint;
            LastSeen = DateTime.Now;
        }

        internal ClusterNodeState(IPEndPoint endpoint, bool isOriginNode)
        {
            Epoch = (DateTime.UtcNow - new DateTime(1970, 1, 1)).Ticks / TimeSpan.TicksPerSecond;
            HeartBeat = 1;            
            EndPoint = endpoint;
            LastSeen = DateTime.Now;
        }

        internal void Update()
        {
            lock (_syncRoot)
            {
                Update(HeartBeat + 1, 0);
            }
        }

        internal void Update(int heartBeat, int errorCount = 0)
        {
            lock (_syncRoot)
            {
                HeartBeat = heartBeat;
                LastSeen = DateTime.Now;
                ErrorCount = errorCount;
            }
        }

        internal void Synchronize(ClusterNodeState node)
        {
            lock (_syncRoot)
            {
                Epoch = node.Epoch;
                Update(node.HeartBeat);
            }
        }

        public bool Equals(ClusterNodeState other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return IPEndPoint.Equals(this.EndPoint, other.EndPoint);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ClusterNodeState);
        }

        public override int GetHashCode()
        {
            return EndPoint.GetHashCode();
        }
    }
}
