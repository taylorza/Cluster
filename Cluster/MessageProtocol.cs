using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cluster
{
    public class MessageProtocol
    {
        private static byte[] EmptyCommand = new byte[0];

        private const int PREFIX_LENGTH = sizeof(int);
        private ByteBuffer _buffer = new ByteBuffer();
        private State _currentState = State.ReadLength;
        private int _messageLength;
        private byte[] _lengthBuffer = new byte[PREFIX_LENGTH];

        public EventHandler<MessageEventArgs> MessageArrived;

        public static byte[] PackMessage(byte[] payload)
        {
            return PackMessage(null, payload);
        }

        public static byte[] PackMessage(byte[] command, byte[] payload)
        {
            if (command == null) command = EmptyCommand;

            byte[] packedMessage = new byte[PREFIX_LENGTH + command.Length + payload.Length];
            byte[] lengthBytes = BitConverter.GetBytes(payload.Length);
            
            Buffer.BlockCopy(lengthBytes, 0, packedMessage, 0, PREFIX_LENGTH);
            Buffer.BlockCopy(command, 0, packedMessage, PREFIX_LENGTH, command.Length);
            Buffer.BlockCopy(payload, 0, packedMessage, PREFIX_LENGTH + command.Length, payload.Length);
            return packedMessage;
        }

        public void DataReceived(byte[] data, int offset, int count)
        {
            _buffer.Enqueue(data, offset, count);

            var entryState = _currentState;
            do
            {
                entryState = _currentState;
                switch (_currentState)
                {
                    case State.ReadLength:
                        if (_buffer.Count >= PREFIX_LENGTH)
                        {
                            Debug.WriteLine("Read Message Length");
                            _buffer.Dequeue(_lengthBuffer);
                            _messageLength = BitConverter.ToInt32(_lengthBuffer, 0);
                            _currentState = State.ReadMessage;
                        }
                        break;

                    case State.ReadMessage:
                        {
                            Debug.WriteLine($"Read Message - {_buffer.Count} / {_messageLength}");
                            if (_buffer.Count >= _messageLength)
                            {
                                var message = new byte[_messageLength];
                                _buffer.Dequeue(message);
                                OnMessageReceived(new MessageEventArgs(message));
                                _currentState = State.ReadLength;
                            }
                        }
                        break;
                }
            } while (_currentState != entryState);
        }

        protected virtual void OnMessageReceived(MessageEventArgs e)
        {
            MessageArrived?.Invoke(this, e);
        }

        private enum State
        {
            ReadLength,
            ReadMessage
        }
    }

    public class MessageEventArgs : EventArgs
    {
        public byte[] Message { get; private set; }

        public MessageEventArgs(byte[] message)
        {
            Message = message;
        }
    }
}
