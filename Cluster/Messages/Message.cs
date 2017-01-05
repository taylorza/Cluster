using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster.Messages
{
    [Serializable]
    class Message : IMessage, IEquatable<Message>
    {
        [NonSerialized]
        private DateTime _lastSeen;

        [NonSerialized]
        private int _duplicateCount;

        public Message()
        {
            MessageId = Guid.NewGuid();
            IgnoreIfDuplicate = true;
            DuplicateCount = 0;
            MaxRelayCount = -1;
        }

        public Guid MessageId { get; }

        public int MaxRelayCount { get; set; }
        
        public DateTime LastSeen
        {
            get { return _lastSeen; }
            set { _lastSeen = value; }
        }

        public int DuplicateCount
        {
            get { return _duplicateCount; }
            set { _duplicateCount = value; }
        }

        public bool IgnoreIfDuplicate { get; set; }

        public bool Equals(Message other)
        {
            return MessageId == other.MessageId;
        }

        public override int GetHashCode()
        {
            return MessageId.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as Message);
        }
    }
}
