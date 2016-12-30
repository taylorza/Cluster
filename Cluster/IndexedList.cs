using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cluster
{
    class IndexedList<T> : IList<T>, IList
    {
        private List<T> _list = new List<T>();
        private Dictionary<T, int> _listIndex = new Dictionary<T, int>();
        private Object _syncRoot;
        
        private void ThrowIfError(T item)
        {
            if (item == null) throw new ArgumentNullException("item");
            if (_listIndex.ContainsKey(item)) throw new ArgumentException("Item already exits", "item");
        }

        public T this[int index]
        {
            get { return _list[index]; }
            set
            {
                T current = _list[index];
                _list[index] = value;
                _listIndex.Remove(current);
                _listIndex.Add(value, index);
            }
        }

        public int Count
        {
            get
            {
                return _list.Count;
            }
        }

        bool ICollection<T>.IsReadOnly
        {
            get
            {
                return false;
            }
        }

        bool IList.IsFixedSize
        {
            get
            {
                return false;
            }
        }

        object ICollection.SyncRoot
        {
            get
            {
                if (_syncRoot == null)
                {
                    Interlocked.CompareExchange<object>(ref _syncRoot, new Object(), null);
                }
                return _syncRoot;
            }
        }

        bool ICollection.IsSynchronized
        {
            get
            {
                return false;
            }
        }

        bool IList.IsReadOnly
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        object IList.this[int index]
        {
            get
            {
                return this[index];
            }

            set
            {
                try
                {
                    this[index] = (T)value;
                }
                catch(InvalidCastException)
                {
                    throw new ArgumentException("Can only add items of type : " + typeof(T).Name);
                }
            }
        }

        public void Add(T item)
        {
            ThrowIfError(item);

            _listIndex.Add(item, _list.Count);
            _list.Add(item);
        }
        
        public void Clear()
        {
            _list.Clear();
            _listIndex.Clear();
        }

        public bool Contains(T item)
        {
            return _listIndex.ContainsKey(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _list.CopyTo(array, arrayIndex);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        public int IndexOf(T item)
        {
            int index;
            if (!_listIndex.TryGetValue(item, out index))
            {
                index = -1;
            }
            return index;
        }

        public void Insert(int index, T item)
        {
            ThrowIfError(item);
            _listIndex.Add(item, index);
            _list.Insert(index, item);
        }

        public bool Remove(T item)
        {
            int index = IndexOf(item);
            if (index >= 0)
            {
                RemoveAt(index);                
            }
            return index >= 0;
        }

        public void RemoveAt(int index)
        {
            if (Count == 1)
            {
                _list.Clear();
                _listIndex.Clear();
                return;
            }

            T item = _list[index];
            if (index == _listIndex.Count - 1)
            {
                _listIndex.Remove(item);
                _list.RemoveAt(index);
                return;
            }

            // Remove an item by swapping it with the last item in the list
            // and then removing the last item from the list. 
            // This makes a O(n) operation a O(1) but sacrifices the order of the items in the list
            T lastItem = _list[_list.Count - 1];
            
            _list[index] = _list[_list.Count - 1];
            _listIndex[lastItem] = index;

            _listIndex.Remove(item);            
            _list.RemoveAt(_list.Count - 1);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        int IList.Add(object value)
        {
            try
            {
                Add((T)value);
                return _list.Count - 1;
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException("Can only add items of type : " + typeof(T).Name);
            }
        }

        bool IList.Contains(object value)
        {
            try
            {
                return IndexOf((T)value) >= 0;
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException("Must be of type : " + typeof(T).Name, "value");
            }
        }

        int IList.IndexOf(object value)
        {
            try
            {
                return IndexOf((T)value);
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException("Must be of type : " + typeof(T).Name, "value");
            }
        }

        void IList.Insert(int index, object value)
        {
            try
            {
                Insert(index, (T)value);
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException("Can only add items of type : " + typeof(T).Name);
            }
        }

        void IList.Remove(object value)
        {
            try
            {
                Remove((T)value);
            }
            catch (InvalidCastException)
            {                
            }            
        }

        void ICollection.CopyTo(Array array, int index)
        {
            ((IList)this).CopyTo(array, index);
        }

        public T[] ToArray()
        {
            return _list.ToArray();
        }
    }
}
