using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class ShareClusterNodes : IMessage
    {
        public ClusterNode[] Nodes { get; private set; }

        public ShareClusterNodes(ClusterNode[] nodes)
        {
            Nodes = nodes;
        }
    }
}
