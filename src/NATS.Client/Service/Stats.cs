﻿// Copyright 2022 The NATS Authors
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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
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
        public long ProcessingTime => processingTime.Read(); 
        public long AverageProcessingTime => averageProcessingTime.Read();
        public IStatsData Data { get; set; }
        public DateTime Started { get; private set; }

        private readonly InterlockedLong numRequests;
        private readonly InterlockedLong numErrors;
        private readonly InterlockedLong processingTime;
        private readonly InterlockedLong averageProcessingTime;

        internal Stats(string serviceId, string name, string version)
        {
            ServiceId = serviceId;
            Name = name;
            Version = version;
            numRequests = new InterlockedLong();
            numErrors = new InterlockedLong();
            LastError = null;
            processingTime = new InterlockedLong();
            averageProcessingTime = new InterlockedLong();
            Data = null;
            Started = DateTime.UtcNow;
        }

        internal Stats Copy(StatsDataDecoder decoder)
        {
            Stats copy = new Stats(ServiceId, Name, Version);
            copy.numRequests.Set(numRequests.Read());
            copy.numErrors.Set(numErrors.Read());
            copy.LastError = LastError;
            copy.processingTime.Set(processingTime.Read());
            copy.averageProcessingTime.Set(averageProcessingTime.Read());
            if (Data != null && decoder != null)
            {
                copy.Data = decoder.Invoke(Data.ToJson());
            }
            copy.Started = Started;
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
            processingTime = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.ProcessingTime));
            averageProcessingTime = new InterlockedLong(JsonUtils.AsLongOrZero(node, ApiConstants.AverageProcessingTime));

            if (decoder != null)
            {
                JSONNode dataNode = node[ApiConstants.Data];
                if (dataNode != null)
                {
                    string dataJson = dataNode.ToString(); // generically decode it
                    if (!string.IsNullOrEmpty(dataJson))
                    {
                        Data = decoder.Invoke(dataJson);
                    }
                }
            }
            
            Started = JsonUtils.AsDate(node[ApiConstants.Started]);
        }
        
        public void Reset()
        {
            numRequests.Set(0);
            numErrors.Set(0);
            LastError = null;
            processingTime.Set(0);
            averageProcessingTime.Set(0);
            Data = null;
            Started = DateTime.UtcNow;
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
            JsonUtils.AddField(jso, ApiConstants.ProcessingTime, processingTime.Read());
            JsonUtils.AddField(jso, ApiConstants.AverageProcessingTime, averageProcessingTime.Read());
            if (Data != null)
            {
                jso[ApiConstants.Data] = JSON.Parse(Data.ToJson());
            }
            JsonUtils.AddField(jso, ApiConstants.Started, Started);
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
            return processingTime.Add(elapsed);
        }

        public void SetAverageProcessingTime(long average) {
            averageProcessingTime.Set(average);
        }
    }
}
