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
    public delegate JsonSerializable StatsDataSupplier();
    public delegate JsonSerializable StatsDataDecoder (string json);

    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Stats : JsonSerializable
    {
        public string ServiceId { get; }
        public string Name { get; }
        public string Version { get; }
        public long NumRequests => numRequests.Read(); 
        public long NumErrors => numErrors.Read(); 
        public string LastError { get; set; }
        public long TotalProcessingTime => totalProcessingTime.Read(); 
        public long AverageProcessingTime => averageProcessingTime.Read();
        public JsonSerializable Data { get; set; }

        private readonly InterlockedLong numRequests;
        private readonly InterlockedLong numErrors;
        private readonly InterlockedLong totalProcessingTime;
        private readonly InterlockedLong averageProcessingTime;

        internal Stats(string serviceId, string name, string version)
        {
            ServiceId = serviceId;
            Name = name;
            Version = version;
            numRequests = new InterlockedLong();
            numErrors = new InterlockedLong();
            LastError = null;
            totalProcessingTime = new InterlockedLong();
            averageProcessingTime = new InterlockedLong();
            Data = null;
        }

        internal Stats Copy(StatsDataDecoder decoder)
        {
            Stats copy = new Stats(ServiceId, Name, Version);
            copy.numRequests.Set(numRequests.Read());
            copy.numErrors.Set(numErrors.Read());
            copy.LastError = LastError;
            copy.totalProcessingTime.Set(totalProcessingTime.Read());
            copy.averageProcessingTime.Set(averageProcessingTime.Read());
            if (Data != null && decoder != null)
            {
                copy.Data = decoder.Invoke(Data.ToJsonNode().ToString());
            }

            return copy;
        }

        internal Stats(string json, StatsDataDecoder decoder) : this(JSON.Parse(json), decoder) {}

        internal Stats(JSONNode node, StatsDataDecoder decoder)
        {
            ServiceId = node[ApiConstants.Id];
            Name = node[ApiConstants.Name];
            Version = node[ApiConstants.Version];

            numRequests = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.NumRequests));
            numErrors = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.NumErrors));
            LastError = node[ApiConstants.LastError];
            totalProcessingTime = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.TotalProcessingTime));
            averageProcessingTime = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.AverageProcessingTime));

            if (decoder != null)
            {
                string dataJson = node[ApiConstants.Data].ToString(); // generically decode it
                if (!string.IsNullOrEmpty(dataJson))
                {
                    Data = decoder.Invoke(dataJson);
                }
            }
        }
        
        public void Reset()
        {
            numRequests.Set(0);
            numErrors.Set(0);
            LastError = null;
            totalProcessingTime.Set(0);
            averageProcessingTime.Set(0);
            Data = null;
        }
        
        internal override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, ServiceId);
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Version, Version);
            JsonUtils.AddField(jso, ApiConstants.NumRequests, numRequests.Read());
            JsonUtils.AddField(jso, ApiConstants.NumErrors, numErrors.Read());
            JsonUtils.AddField(jso, ApiConstants.LastError, LastError);
            JsonUtils.AddField(jso, ApiConstants.TotalProcessingTime, totalProcessingTime.Read());
            JsonUtils.AddField(jso, ApiConstants.AverageProcessingTime, averageProcessingTime.Read());
            JsonUtils.AddField(jso, ApiConstants.Data, Data);
            return jso;
        }

        public long IncrementNumRequests()
        {
            return numRequests.Increment();
        }

        public void IncrementNumErrors()
        {
            numErrors.Increment();
        }

        public long AddTotalProcessingTime(long elapsed)
        {
            return totalProcessingTime.Add(elapsed);
        }

        public void SetAverageProcessingTime(long average) {
            averageProcessingTime.Set(average);
        }
    }
}
