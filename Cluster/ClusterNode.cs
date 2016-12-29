﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    [Serializable]
    class ClusterNode : IEquatable<ClusterNode>
    {
        [NonSerialized]
        private DateTime _lastSeen;

        [NonSerialized]
        private int _errorCount;

        public bool IsOriginNode { get; private set; }
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
            set { _errorCount = value; }
        }

        public ClusterNode(IPEndPoint endpoint, bool isOriginNode)
        {
            HeartBeat = 1;
            EndPoint = endpoint;
            LastSeen = DateTime.Now;
            IsOriginNode = isOriginNode;
        }

        public void Update()
        {
            Update(HeartBeat + 1);
        }

        public void Update(int heartBeat)
        {
            HeartBeat = heartBeat;
            LastSeen = DateTime.Now;
            ErrorCount = 0;
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