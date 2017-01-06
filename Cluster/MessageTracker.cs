using Cluster.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    class MessageTracker
    {
        private object _syncRoot = new object();

        private ConcurrentDictionary<Guid, MessageEntry> _messages = new ConcurrentDictionary<Guid, MessageEntry>();

        public int TrackMessage(IMessage message)
        {
            var messageEntry = _messages.GetOrAdd(message.MessageId, (id) => { return new MessageEntry(message); });
            return messageEntry.Update();
        }

        public void Scavange()
        {
            List<Guid> expiredMessages = new List<Guid>();

            foreach(var messageEntry in _messages)
            {
                if (messageEntry.Value.Age > 60)
                {
                    expiredMessages.Add(messageEntry.Key);
                }
            }

            foreach(var key in expiredMessages)
            {
                MessageEntry messageEntry;
                _messages.TryRemove(key, out messageEntry);
            }
        }

        class MessageEntry
        {
            private object _syncRoot = new object();

            public IMessage Message { get; private set; }
            public DateTime LastSeen { get; private set; }
            public int RelayCount {get; private set;}            

            public MessageEntry(IMessage message)
            {
                Message = message;
                LastSeen = DateTime.Now;
                RelayCount = 0;
            }

            public int Update()
            {
                lock(_syncRoot)
                {
                    LastSeen = DateTime.Now;
                    RelayCount++;
                    return RelayCount;
                }
            }

            public double Age
            {
                get
                {
                    lock(_syncRoot)
                    {
                        return (DateTime.Now - LastSeen).TotalSeconds;
                    }
                }
            }
        }
    }
}
