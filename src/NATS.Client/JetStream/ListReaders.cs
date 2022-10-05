// Copyright 2021-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
using System.Text;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    internal class ListRequestEngine : ApiResponse
    {
        private static readonly string OffsetJsonStart = "{\"offset\":";

        protected readonly int Total; // so always has the first "at least one more"
        protected readonly int Limit;
        protected readonly int LastOffset;

        internal ListRequestEngine() : base()
        {
            Total = Int32.MaxValue;
            Limit = 0;
            LastOffset = 0;
        }

        internal ListRequestEngine(Msg msg) : base(msg, true)
        {
            Total = JsonNode.GetValueOrDefault(ApiConstants.Total, Int32.MaxValue);
            Limit = JsonNode.GetValueOrDefault(ApiConstants.Limit, 0);
            LastOffset = JsonNode.GetValueOrDefault(ApiConstants.Offset, 0);
        }

        internal bool HasMore()
        {
            return Total > NextOffset();
        }

        internal byte[] InternalNextJson()
        {
            return HasMore() ? NoFilterJson() : null;
        }

        internal byte[] NoFilterJson()
        {
            return Encoding.ASCII.GetBytes(OffsetJsonStart + NextOffset() + "}");
        }

        internal byte[] InternalNextJson(string fieldName, string filter)
        {
            if (HasMore())
            {
                if (filter == null)
                {
                    return NoFilterJson();
                }

                return Encoding.ASCII.GetBytes(OffsetJsonStart + NextOffset() + ",\"" + fieldName + "\":\"" +
                                               filter + "\"}");
            }

            return null;
        }

        internal int NextOffset()
        {
            return (LastOffset + Limit);
        }

        internal JSONArray GetNodes(string objectName)
        {
            return JsonNode[objectName].AsArray;
        }
    }

    internal abstract class AbstractListReader
    {
        private readonly string objectName;
        private readonly string filterFieldName;

        protected ListRequestEngine Engine;

        internal void Process(Msg msg)
        {
            Engine = new ListRequestEngine(msg);
            JSONArray nodes = Engine.GetNodes(objectName);
            if (nodes != null)
            {
                for (int x = 0; x < nodes.Count; x++)
                {
                    ProcessItem(nodes[x]);
                }
            }
        }

        protected abstract void ProcessItem(JSONNode node);

        protected AbstractListReader(string objectName, string filterFieldName = null)
        {
            this.objectName = objectName;
            this.filterFieldName = filterFieldName;
            Engine = new ListRequestEngine();
        }

        internal byte[] NextJson()
        {
            return Engine.InternalNextJson();
        }

        internal byte[] NextJson(string filter)
        {
            if (filterFieldName == null)
            {
                throw new ArgumentException("Filter not supported.");
            }

            return Engine.InternalNextJson(filterFieldName, filter);
        }

        internal bool HasMore()
        {
            return Engine.HasMore();
        }
    }

    internal abstract class StringListReader : AbstractListReader
    {
        private List<string> _strings = new List<string>();

        protected StringListReader(string objectName, string filterFieldName = null) : base(objectName, filterFieldName) {}

        protected override void ProcessItem(JSONNode node)
        {
            _strings.Add(node.Value);
        }

        public List<string> Strings => _strings;
    }

    internal class ConsumerNamesReader : StringListReader
    {
        internal ConsumerNamesReader() : base(ApiConstants.Consumers) {}
    }

    internal class StreamNamesReader : StringListReader
    {
        internal StreamNamesReader() : base(ApiConstants.Streams, ApiConstants.Subject) {}
    }

    internal class ConsumerListReader : AbstractListReader
    {
        private List<ConsumerInfo> _consumerInfos = new List<ConsumerInfo>();

        internal ConsumerListReader() : base(ApiConstants.Consumers) {}

        protected override void ProcessItem(JSONNode node)
        {
            _consumerInfos.Add(new ConsumerInfo(node));
        }

        public List<ConsumerInfo> Consumers => _consumerInfos;
    }

    internal class StreamListReader : AbstractListReader
    {
        private List<StreamInfo> _streamInfos = new List<StreamInfo>();

        internal StreamListReader() : base(ApiConstants.Streams, ApiConstants.Subject) {}

        protected override void ProcessItem(JSONNode node)
        {
            _streamInfos.Add(new StreamInfo(node));
        }

        public List<StreamInfo> Streams => _streamInfos;
    }

    internal class StreamInfoReader
    {
        internal StreamInfo StreamInfo { get; private set; }
        private ListRequestEngine engine;

        public StreamInfoReader()
        {
            engine = new ListRequestEngine();
        }

        internal void Process(Msg msg)
        {
            engine = new ListRequestEngine(msg);
            StreamInfo si = new StreamInfo(msg, true);
            if (StreamInfo == null)
            {
                StreamInfo = si;
            }
            else
            {
                StreamInfo.State.AddAll(si.State.Subjects);
            }
        }

        internal byte[] NextJson(StreamInfoOptions options)
        {
            JSONNode node = new JSONObject();
            node["offset"] = engine.NextOffset();

            if (options != null)
            {
                if (options.SubjectsFilter != null)
                {
                    node[ApiConstants.SubjectsFilter] = options.SubjectsFilter;
                }

                if (options.DeletedDetails)
                {
                    node[ApiConstants.DeletedDetails] = true;
                }
            }

            return Encoding.ASCII.GetBytes(node.ToString());
        }

        internal bool HasMore()
        {
            return engine.HasMore();
        }
    }
}