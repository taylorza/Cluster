using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class JoinClusterRequest : IMessage
    {
        public string ClusterId { get; private set; }
        public ClusterNode Node { get; private set; }
       
        public JoinClusterRequest(string clusterId, ClusterNode node)
        {
            ClusterId = clusterId;
            Node = node;
        }
    }
}
