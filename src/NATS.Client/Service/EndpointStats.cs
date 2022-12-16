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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class EndpointStats  : JsonSerializable, IComparable<EndpointStats>
    {   
        public string Name { get; }
        public long NumRequests => numRequests.Read(); 
        public long NumErrors => numErrors.Read(); 
        public string LastError { get; internal set;  }
        public long TotalProcessingTime => totalProcessingTime.Read(); 
        public long AverageProcessingTime => averageProcessingTime.Read();
        public EndpointStatsData EndpointStatsData { get; }

        private InterlockedLong numRequests;
        private InterlockedLong numErrors;
        private InterlockedLong totalProcessingTime;
        private InterlockedLong averageProcessingTime;

        public EndpointStats(string name)
        {
            Name = name;
            Reset();
            EndpointStatsData = null;  // still have no info for data
        }

        internal EndpointStats(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Reset();
            EndpointStatsData = null;  // still have no info for data

            numRequests.Set(JsonUtils.AsLongOrZero(node, ApiConstants.NumRequests));
            numErrors.Set(JsonUtils.AsLongOrZero(node, ApiConstants.NumErrors));
            LastError = node[ApiConstants.LastError];
            totalProcessingTime.Set(JsonUtils.AsLongOrZero(node, ApiConstants.TotalProcessingTime));
            averageProcessingTime.Set(JsonUtils.AsLongOrZero(node, ApiConstants.AverageProcessingTime));
        }

        internal static IList<EndpointStats> ToList(JSONNode listNode)
        {
            IList<EndpointStats> list =  new List<EndpointStats>();
            if (listNode != null)
            {
                foreach (var esNode in listNode.Children)
                {
                    list.Add(new EndpointStats(esNode));
                }
            }

            return list;
        }
        
        public void Reset()
        {
            numRequests = new InterlockedLong();
            numErrors = new InterlockedLong();
            LastError = null;
            totalProcessingTime = new InterlockedLong();
            averageProcessingTime = new InterlockedLong();
        }
        
        internal override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.NumRequests, numRequests.Read());
            JsonUtils.AddField(jso, ApiConstants.NumErrors, numErrors.Read());
            JsonUtils.AddField(jso, ApiConstants.LastError, LastError);
            JsonUtils.AddField(jso, ApiConstants.TotalProcessingTime, totalProcessingTime.Read());
            JsonUtils.AddField(jso, ApiConstants.AverageProcessingTime, averageProcessingTime.Read());
            return jso;
        }

        public override string ToString()
        {
            return $"Name: {Name}, NumRequests: {NumRequests}, NumErrors: {NumErrors}, LastError: {LastError}, TotalProcessingTime: {TotalProcessingTime}, AverageProcessingTime: {AverageProcessingTime}, EndpointStatsData: {EndpointStatsData}";
        }

        public int CompareTo(EndpointStats other)
        {
            return Order(this).CompareTo(Order(other));
        }

        private static int Order(EndpointStats es) {
            switch (es.Name) {
                case ServiceUtil.Ping: return 1;
                case ServiceUtil.Info: return 2;
                case ServiceUtil.Schema: return 3;
                case ServiceUtil.Stats: return 4;
            }
            return 0;
        }
    }
}
