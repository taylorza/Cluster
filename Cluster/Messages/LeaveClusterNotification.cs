using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class LeaveClusterNotification : Message
    {
        public string ClusterId { get; private set; }
        public ClusterNodeState Node { get; private set; }
       
        public LeaveClusterNotification(string clusterId, ClusterNodeState node)
        {            
            ClusterId = clusterId;
            Node = node;
            MaxRelayCount = 3;
        }
    }
}
