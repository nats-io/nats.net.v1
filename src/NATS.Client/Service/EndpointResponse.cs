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
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class EndpointResponse : JsonSerializable
    {
        public string Name { get; }
        public string Subject { get; }
        public Schema Schema { get; }
        public long NumRequests { get; } 
        public long NumErrors { get; } 
        public long ProcessingTime { get; } 
        public long AverageProcessingTime { get; }
        public string LastError { get; }
        public JSONNode Data { get; }
        public DateTime Started { get; }

        internal EndpointResponse(string name, string subject, long numRequests, long numErrors, long processingTime, string lastError, JSONNode data, DateTime started) {
            Name = name;
            Subject = subject;
            Schema = null;
            NumRequests = numRequests;
            NumErrors = numErrors;
            ProcessingTime = processingTime;
            AverageProcessingTime = numRequests < 1 ? 0 : processingTime / numRequests;
            LastError = lastError;
            Data = data;
            Started = started;
        }

        internal EndpointResponse(string name, string subject, Schema schema) {
            Name = name;
            Subject = subject;
            Schema = schema;
            NumRequests = 0;
            NumErrors = 0;
            ProcessingTime = 0;
            AverageProcessingTime = 0;
            LastError = null;
            Data = null;
            Started = DateTime.MinValue;
        }

        internal EndpointResponse(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Subject = node[ApiConstants.Subject];
            Schema = Schema.OptionalInstance(node[ApiConstants.Schema]);
            NumRequests = JsonUtils.AsLongOrZero(node, ApiConstants.NumRequests);
            NumErrors = JsonUtils.AsLongOrZero(node, ApiConstants.NumErrors);
            ProcessingTime = JsonUtils.AsLongOrZero(node, ApiConstants.ProcessingTime);
            AverageProcessingTime = JsonUtils.AsLongOrZero(node, ApiConstants.AverageProcessingTime);
            LastError = node[ApiConstants.LastError];
            Data = node[ApiConstants.Data];
            Started = JsonUtils.AsDate(node[ApiConstants.Started]); 
        }

        internal static IList<EndpointResponse> ListOf(JSONNode listNode)
        {
            IList<EndpointResponse> list = new List<EndpointResponse>();
            if (listNode != null)
            {
                foreach (var esNode in listNode.Children)
                {
                    list.Add(new EndpointResponse(esNode));
                }
            }
            return list;
        }
        
        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Subject, Subject);
            JsonUtils.AddField(jso, ApiConstants.Schema, Schema);
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
        
        protected bool Equals(EndpointResponse other)
        {
            return Name == other.Name 
                   && Subject == other.Subject 
                   && Equals(Schema, other.Schema) 
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
            return Equals((EndpointResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Subject != null ? Subject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Schema != null ? Schema.GetHashCode() : 0);
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
