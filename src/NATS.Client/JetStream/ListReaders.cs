// Copyright 2021 The NATS Authors
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
        private static readonly String OffsetJsonStart = "{\"offset\":";

        protected readonly int total; // so always has the first "at least one more"
        protected readonly int limit;
        protected readonly int lastOffset;

        internal ListRequestEngine() : base()
        {
            total = Int32.MaxValue;
            limit = 0;
            lastOffset = 0;
        }

        internal ListRequestEngine(Msg msg) : base(msg, true)
        {
            total = JsonNode.GetValueOrDefault(ApiConstants.Total, Int32.MaxValue);
            limit = JsonNode.GetValueOrDefault(ApiConstants.Limit, 0);
            lastOffset = JsonNode.GetValueOrDefault(ApiConstants.Offset, 0);
        }

        internal bool HasMore()
        {
            return total > (lastOffset + limit);
        }

        internal byte[] InternalNextJson()
        {
            return HasMore() ? NoFilterJson() : null;
        }

        internal byte[] NoFilterJson()
        {
            return Encoding.ASCII.GetBytes(OffsetJsonStart + (lastOffset + limit) + "}");
        }

        internal byte[] InternalNextJson(String fieldName, String filter)
        {
            if (HasMore())
            {
                if (filter == null)
                {
                    return NoFilterJson();
                }

                return Encoding.ASCII.GetBytes(OffsetJsonStart + (lastOffset + limit) + ",\"" + fieldName + "\":\"" +
                                               filter + "\"}");
            }

            return null;
        }

        internal JSONArray GetItems(String objectName)
        {
            return JsonNode[objectName].AsArray;
        }
    }

    internal abstract class AbstractListReader
    {
        private readonly string objectName;
        private readonly string filterFieldName;

        protected ListRequestEngine engine;

        internal void Process(Msg msg)
        {
            engine = new ListRequestEngine(msg);
            ProcessItems(engine.GetItems(objectName));
        }

        protected abstract void ProcessItems(JSONArray items);

        protected AbstractListReader(string objectName, string filterFieldName = null)
        {
            this.objectName = objectName;
            this.filterFieldName = filterFieldName;
            engine = new ListRequestEngine();
        }

        internal byte[] NextJson()
        {
            return engine.InternalNextJson();
        }

        internal byte[] NextJson(string filter)
        {
            if (filterFieldName == null)
            {
                throw new ArgumentException("Filter not supported.");
            }

            return engine.InternalNextJson(filterFieldName, filter);
        }

        internal bool HasMore()
        {
            return engine.HasMore();
        }
    }

    internal abstract class StringListReader : AbstractListReader
    {
        private List<string> _strings = new List<string>();

        protected StringListReader(String objectName, String filterFieldName = null) : base(objectName, filterFieldName) {}

        protected override void ProcessItems(JSONArray items)
        {
            for (int x = 0; x < items.Count; x++)
            {
                _strings.Add(items[x].Value);
            }
        }

        public List<string> Strings => _strings;
    }

    internal class ConsumerNamesReader : StringListReader
    {
        internal ConsumerNamesReader() : base(ApiConstants.Consumers) {}
    }

    internal class StreamNamesReader : StringListReader
    {
        internal StreamNamesReader() : base(ApiConstants.Streams) {}
    }

    internal class ConsumerListReader : AbstractListReader
    {
        private List<ConsumerInfo> _consumerInfos = new List<ConsumerInfo>();

        internal ConsumerListReader() : base(ApiConstants.Consumers) {}

        protected override void ProcessItems(JSONArray items)
        {
            for (int x = 0; x < items.Count; x++)
            {
                _consumerInfos.Add(new ConsumerInfo(items[x]));
            }
        }

        public List<ConsumerInfo> Consumers => _consumerInfos;
    }

    internal class StreamListReader : AbstractListReader
    {
        private List<StreamInfo> _streamInfos = new List<StreamInfo>();

        internal StreamListReader() : base(ApiConstants.Streams) {}

        protected override void ProcessItems(JSONArray items)
        {
            for (int x = 0; x < items.Count; x++)
            {
                _streamInfos.Add(new StreamInfo(items[x]));
            }
        }

        public List<StreamInfo> Streams => _streamInfos;
    }
}