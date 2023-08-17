// Copyright 2023 The NATS Authors
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
    /// Endpoints stats contains various stats and custom data for an endpoint.
    /// <code>
    /// {
    /// "id": "ZP1oVevzLGu4CBORMXKKke",
    /// "name": "Service1",
    /// "version": "0.0.1",
    /// "endpoints": [{
    ///     "name": "SortEndpointAscending",
    ///     "subject": "sort.ascending",
    ///     "num_requests": 1,
    ///     "processing_time": 538900,
    ///     "average_processing_time": 538900,
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }
    /// </code>
    /// <code>
    /// {
    ///     "name": "SortEndpointDescending",
    ///     "subject": "sort.descending",
    ///     "num_requests": 1,
    ///     "processing_time": 88400,
    ///     "average_processing_time": 88400,
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }
    /// </code>
    /// <code>
    /// {
    ///     "name": "EchoEndpoint",
    ///     "subject": "echo",
    ///     "num_requests": 5,
    ///     "processing_time": 1931600,
    ///     "average_processing_time": 386320,
    ///     "data": {
    ///          "idata": 2,
    ///          "sdata": "s-996409223"
    ///     },
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }
    /// </code>
    /// </summary>
    public class EndpointStats : JsonSerializable
    {
        /// <value>Get the name of the Endpoint</value>
        public string Name { get; }
        
        /// <value>Get the subject of the Endpoint</value>
        public string Subject { get; }
            
        /// <value>The number of requests received by the endpoint</value>
        public long NumRequests { get; } 
        
        /// <value>Number of errors that the endpoint has raised</value>
        public long NumErrors { get; } 
        
        /// <value>Total processing time for the endpoint</value>
        public long ProcessingTime { get; } 
            
        /// <value>Average processing time is the total processing time divided by the num requests</value>
        public long AverageProcessingTime { get; }

        /// <value>If set, the last error triggered by the endpoint</value>
        public string LastError { get; }

        /// <value>A field that can be customized with any data as returned by stats handler</value>
        public JSONNode Data { get; }

        /// <value>The json representation of the custom data. May be null</value>
        public string DataAsJson => Data == null ? null : Data.ToString();
        
        /// <value>Get the time the endpoint was started (or restarted)</value>
        public DateTime Started { get; }

        internal EndpointStats(string name, string subject, long numRequests, long numErrors, long processingTime, string lastError, JSONNode data, DateTime started) {
            Name = name;
            Subject = subject;
            NumRequests = numRequests;
            NumErrors = numErrors;
            ProcessingTime = processingTime;
            AverageProcessingTime = numRequests < 1 ? 0 : processingTime / numRequests;
            LastError = lastError;
            Data = data;
            Started = started;
        }

        internal EndpointStats(string name, string subject) {
            Name = name;
            Subject = subject;
            NumRequests = 0;
            NumErrors = 0;
            ProcessingTime = 0;
            AverageProcessingTime = 0;
            LastError = null;
            Data = null;
            Started = DateTime.MinValue;
        }

        internal EndpointStats(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Subject = node[ApiConstants.Subject];
            NumRequests = JsonUtils.AsLongOrZero(node, ApiConstants.NumRequests);
            NumErrors = JsonUtils.AsLongOrZero(node, ApiConstants.NumErrors);
            ProcessingTime = JsonUtils.AsLongOrZero(node, ApiConstants.ProcessingTime);
            AverageProcessingTime = JsonUtils.AsLongOrZero(node, ApiConstants.AverageProcessingTime);
            LastError = node[ApiConstants.LastError];
            Data = node[ApiConstants.Data];
            Started = JsonUtils.AsDate(node[ApiConstants.Started]); 
        }

        internal static IList<EndpointStats> ListOf(JSONNode listNode)
        {
            IList<EndpointStats> list = new List<EndpointStats>();
            if (listNode != null)
            {
                foreach (var esNode in listNode.Children)
                {
                    list.Add(new EndpointStats(esNode));
                }
            }
            return list;
        }
        
        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Subject, Subject);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.NumRequests, NumRequests);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.NumErrors, NumErrors);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.ProcessingTime, ProcessingTime);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.AverageProcessingTime, AverageProcessingTime);
            JsonUtils.AddField(jso, ApiConstants.LastError, LastError);
            JsonUtils.AddField(jso, ApiConstants.Data, Data);
            JsonUtils.AddField(jso, ApiConstants.Started, Started);
            return jso;
        }

        public override string ToString()
        {
            return JsonUtils.ToKey(GetType()) + ToJsonString();
        }
        
        protected bool Equals(EndpointStats other)
        {
            return Name == other.Name 
                   && Subject == other.Subject 
                   && NumRequests == other.NumRequests 
                   && NumErrors == other.NumErrors 
                   && ProcessingTime == other.ProcessingTime 
                   && AverageProcessingTime == other.AverageProcessingTime 
                   && LastError == other.LastError 
                   && Data == other.Data 
                   && Started.Equals(other.Started);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((EndpointStats)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Subject != null ? Subject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ NumRequests.GetHashCode();
                hashCode = (hashCode * 397) ^ NumErrors.GetHashCode();
                hashCode = (hashCode * 397) ^ ProcessingTime.GetHashCode();
                hashCode = (hashCode * 397) ^ AverageProcessingTime.GetHashCode();
                hashCode = (hashCode * 397) ^ (LastError != null ? LastError.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Data != null ? Data.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Started.GetHashCode();
                return hashCode;
            }
        }
    }
}
