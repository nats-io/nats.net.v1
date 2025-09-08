// Copyright 2022 The NATS Authors
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

using System;
using System.Collections.Generic;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class Subject
    {
        public string Name { get; }
        public ulong MessageCount { get; }

        [Obsolete("This property is obsolete in favor of MessageCount which is matches better the possible size of the value as kept by the server.", false)]
        public long Count => Convert.ToInt64(MessageCount);

        internal static IList<Subject> GetList(JSONNode subjectsNode)
        {
            List<Subject> list = new List<Subject>();
            if (subjectsNode != null)
            {
                JSONNode.Enumerator e = subjectsNode.GetEnumerator();
                while (e.MoveNext())
                {
                    KeyValuePair<string, JSONNode> pair = e.Current;
                    if (!pair.Value.IsNull)
                    {
                        list.Add(new Subject(pair.Key, pair.Value.AsUlong));
                    }
                }
            }
            return list;
        }

        public Subject(string name, long count)
        {
            Name = name;
            MessageCount = Convert.ToUInt64(count);
        }

        public Subject(string name, ulong count)
        {
            Name = name;
            MessageCount = count;
        }

        public override string ToString()
        {
            return $"{{Name: {Name}, Count: {MessageCount}}}";
        }
    }
}
