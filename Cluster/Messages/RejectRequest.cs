﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class RejectRequest : IMessage
    {
        public RejectRequest()
        {
        }
    }
}
