using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class JoinClusterResponse : IMessage
    {
        public bool JoinSucceeded { get; private set; }
        public ClusterNode[] Nodes { get; private set; }

        public JoinClusterResponse(bool succeeded, ClusterNode[] nodes)
        {
            JoinSucceeded = succeeded;
            Nodes = nodes;
        }        
    }
}
