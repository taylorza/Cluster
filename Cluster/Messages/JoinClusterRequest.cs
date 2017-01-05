using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class JoinClusterRequest : Message
    {
        public string ClusterId { get; private set; }
        public ClusterNodeState Node { get; private set; }
        
        public JoinClusterRequest(string clusterId, ClusterNodeState node)
        {
            ClusterId = clusterId;
            Node = node;
        }
    }
}
