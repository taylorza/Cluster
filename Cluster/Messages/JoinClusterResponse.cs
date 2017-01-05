using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class JoinClusterResponse : Message
    {
        public bool JoinSucceeded { get; private set; }
        public ClusterNodeState[] Nodes { get; private set; }

        public JoinClusterResponse(bool succeeded, ClusterNodeState[] nodes)
        {
            JoinSucceeded = succeeded;
            Nodes = nodes;
        }        
    }
}
