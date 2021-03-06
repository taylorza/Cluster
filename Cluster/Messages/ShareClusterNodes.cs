﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class ShareClusterNodes : Message
    {
        public ClusterNodeState[] Nodes { get; private set; }

        public ShareClusterNodes(ClusterNodeState[] nodes)
        {
            Nodes = nodes;
        }
    }
}
