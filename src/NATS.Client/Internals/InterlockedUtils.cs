// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Threading;

namespace NATS.Client.Internals
{
    public class InterlockedLong
    {
        private long count;

        public InterlockedLong() {}

        public InterlockedLong(long start)
        {
            count = start;
        }

        public void Set(long l)
        {
            Interlocked.Exchange(ref count, l);
        }

        public long Increment()
        {
            return Interlocked.Increment(ref count);
        }

        public long Add(long value)
        {
            if (value == 0)
            {
                return Read();
            }
            return Interlocked.Add(ref count, value);
        }

        public long Read()
        {
            return Interlocked.Read(ref count);
        }
    }

    public class InterlockedInt
    {
        private readonly InterlockedLong _il;

        public InterlockedInt()
        {
            _il = new InterlockedLong();
        }

        public InterlockedInt(int start)
        {
            _il = new InterlockedLong(start);
        }

        public void Set(int i)
        {
            _il.Set(i);
        }

        public int Increment()
        {
            return (int)_il.Increment();
        }

        public long Add(int value)
        {
            return (int)_il.Add(value);
        }

        public int Read()
        {
            return (int)_il.Read();
        }
    }

    public class InterlockedBoolean
    {
        private long _flag;

        public InterlockedBoolean() : this(false) {}

        public InterlockedBoolean(bool flag)
        {
            _flag = flag ? 1 : 0;
        }

        public void Set(bool flag)
        {
            Interlocked.Exchange(ref _flag, flag ? 1 : 0);
        }

        public bool IsTrue()
        {
            return Interlocked.Read(ref _flag) == 1;
        }

        public bool IsFalse()
        {
            return Interlocked.Read(ref _flag) == 0;
        }
    }
}
