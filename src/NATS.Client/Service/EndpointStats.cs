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

using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class EndpointStats  : JsonSerializable
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
    }
}
