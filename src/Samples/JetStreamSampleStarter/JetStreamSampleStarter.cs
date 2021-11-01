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
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This is just a class to get you started on your own...
    /// </summary>
    public static class JetStreamSampleStarter
    {
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStreamSampleStarter", args, "")
                .DefaultStream("starter-stream")
                .DefaultSubject("starter-subject")
                .DefaultPayload("Hello World")
                .DefaultCount(10)
                .Build();
            
            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // create a JetStream context
                    IJetStream js = c.CreateJetStreamContext();
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
           
        }
    }
}