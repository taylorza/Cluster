using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    internal interface IMessage
    {
        Guid MessageId { get; }
        int TimeToLive { get; set; }
        int DuplicateCount { get; set; }
        bool IgnoreIfDuplicate { get; set; }
        DateTime LastSeen { get; set; }   
    }
}
