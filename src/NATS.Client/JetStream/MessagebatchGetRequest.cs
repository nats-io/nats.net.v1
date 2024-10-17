// Copyright 2021 The NATS Authors
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

namespace NATS.Client.JetStream
{
    public sealed class MessageBatchGetRequest : JsonSerializable
    {
        public int Batch { get; }
        public int MaxBytes { get; }
        public ulong MinSequence { get; }
        public DateTime StartTime { get; }
        public string Subject { get; }
        public IList<string> LastBySubjects { get; }
        public ulong UpToSequence { get; }
        public DateTime UpToTime { get; }

        public MessageBatchGetRequest(MessageBatchGetRequestBuilder b)
        {
            Batch = b._batch;
            MaxBytes = b._maxBytes;
            MinSequence = b._minSequence;
            StartTime = b._startTime;
            Subject = b._subject;
            LastBySubjects = b._lastBySubjects;
            UpToSequence = b._upToSequence;
            UpToTime = b._upToTime;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.Batch, Batch);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.MaxBytes, MaxBytes);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.Seq, MinSequence);
            JsonUtils.AddField(jso, ApiConstants.StartTime, StartTime);
            JsonUtils.AddField(jso, ApiConstants.NextBySubject, Subject);
            JsonUtils.AddField(jso, ApiConstants.MultiLast, LastBySubjects);
            JsonUtils.AddFieldWhenGtZero(jso, ApiConstants.UoToSeq, UpToSequence);
            JsonUtils.AddField(jso, ApiConstants.UpToTime, UpToTime);
            return jso;
        }

        public static MessageBatchGetRequestBuilder Builder() => new MessageBatchGetRequestBuilder();

        public sealed class MessageBatchGetRequestBuilder
        {
            internal int _batch = -1;
            internal int _maxBytes = -1;
            internal ulong _minSequence = 0;
            internal DateTime _startTime = DateTime.MinValue;
            internal string _subject;
            internal IList<string> _lastBySubjects;
            internal ulong _upToSequence = 0;
            internal DateTime _upToTime = DateTime.MinValue;

            public MessageBatchGetRequestBuilder WithBatch(int batch)
            {
                _batch = Validator.ValidateGtZero(batch, "Batch");
                return this;
            }
            
            public MessageBatchGetRequestBuilder WithMaxBytes(int maxBytes)
            {
                _maxBytes = maxBytes;
                return this;
            }

            public MessageBatchGetRequestBuilder WithMinSequence(ulong minSequence)
            {
                _minSequence = minSequence;
                return this;
            }

            public MessageBatchGetRequestBuilder WithStartTime(DateTime startTime)
            {
                _startTime = startTime;
                return this;
            }
            
            public MessageBatchGetRequestBuilder WithSubject(string subject)
            {
                _subject = subject;
                return this;
            }

            public MessageBatchGetRequestBuilder WithLastBySubjects(IList<string> lastBySubjects)
            {
                _lastBySubjects = new List<string>(lastBySubjects);
                return this;
            }

            public MessageBatchGetRequestBuilder WithLastBySubjects(params string[] lastBySubjects)
            {
                _lastBySubjects = new List<string>(lastBySubjects);
                return this;
            }

            public MessageBatchGetRequestBuilder WithUpToSequence(ulong upToSequence)
            {
                _upToSequence = upToSequence;
                return this;
            }

            public MessageBatchGetRequestBuilder WithUpToTime(DateTime upToTime)
            {
                _upToTime = upToTime;
                return this;
            }

            public MessageBatchGetRequest Build()
            {
                return new MessageBatchGetRequest(this);
            }
        } 
    }
}
