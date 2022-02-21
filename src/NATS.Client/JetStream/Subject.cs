using System;
using System.Collections.Generic;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class Subject
    {
        public string Name { get; }
        public long Count { get; }

        internal static IList<Subject> OptionalListOf(JSONNode subjectsNode)
        {
            if (subjectsNode == null)
            {
                return null;
            }
            
            List<Subject> list = new List<Subject>();
            JSONNode.Enumerator e = subjectsNode.GetEnumerator();
            while (e.MoveNext())
            {
                KeyValuePair<String, JSONNode> pair = e.Current;
                list.Add(new Subject(pair.Key, pair.Value.AsLong));
            }
            return list.Count == 0 ? null : list;
        }

        public Subject(string name, long count)
        {
            Name = name;
            Count = count;
        }
    }
}