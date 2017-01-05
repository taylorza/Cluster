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
        private ConcurrentDictionary<Guid, MessageEntry> _messages = new ConcurrentDictionary<Guid, MessageEntry>();

        public int TrackMessage(IMessage message)
        {
            var messageEntry = _messages.AddOrUpdate(message.MessageId,
                (_) => { return new MessageEntry(message); },
                (id, m) => { m.RelayCount++; m.LastSeen = DateTime.Now; return m; });

            return messageEntry.RelayCount;
        }

        public void Scavange()
        {
            List<Guid> expiredMessages = new List<Guid>();

            foreach(var messageEntry in _messages)
            {
                var messageAge = (DateTime.Now - messageEntry.Value.LastSeen).TotalSeconds;
                if (messageAge > 60)
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
            public IMessage Message { get; private set; }
            public DateTime LastSeen { get; set; }
            public int RelayCount {get; set;}            

            public MessageEntry(IMessage message)
            {
                Message = message;
                LastSeen = DateTime.Now;
                RelayCount = 1;
            }
        }
    }
}
