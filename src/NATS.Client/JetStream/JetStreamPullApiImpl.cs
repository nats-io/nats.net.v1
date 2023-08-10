// Copyright 2022-2023 The NATS Authors
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

namespace NATS.Client.JetStream
{
    internal class JetStreamPullApiImpl
    {
        private readonly InterlockedLong _pullSubjectIdHolder;
        private readonly Connection _conn;
        private readonly JetStream _js;
        private readonly MessageManager _mm;
        private readonly string _stream;
        private readonly string _subject;
        private string _consumer;

        internal JetStreamPullApiImpl(Connection conn, JetStream js, MessageManager messageManager, 
            string stream, string subject, string consumer)
        {
            _pullSubjectIdHolder = new InterlockedLong();
            _conn = conn;
            _js = js;
            _mm = messageManager;
            _stream = stream;
            _subject = subject;
            _consumer = consumer;
        }

        internal void UpdateConsumer(string consumer)
        {
            _consumer = consumer;
        }
        
        internal string Pull(PullRequestOptions pullRequestOptions, bool raiseStatusWarnings, IPullManagerObserver pullManagerObserver) {
            string publishSubject = _js.PrependPrefix(string.Format(JetStreamConstants.JsapiConsumerMsgNext, _stream, _consumer));
            string pullSubject = _subject.Replace("*", _pullSubjectIdHolder.Increment().ToString());
            _mm.StartPullRequest(pullSubject, pullRequestOptions, raiseStatusWarnings, pullManagerObserver);	
            _conn.Publish(publishSubject, pullSubject, pullRequestOptions.Serialize());
            return pullSubject;
        }
    }
}
