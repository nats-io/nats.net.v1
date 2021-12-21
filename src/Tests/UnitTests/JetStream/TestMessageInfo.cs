// Copyright 2020 The NATS Authors
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

using System.Text;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestMessageInfo : TestBase
    {
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("MessageInfo1.json");
            MessageInfo mi = new MessageInfo(json);
            Assert.Equal("subject", mi.Subject);
            Assert.Equal(1U, mi.Sequence);
            Assert.Equal("data-1", Encoding.UTF8.GetString(mi.Data));
            Assert.Equal(1, mi.Headers.Count);
            Assert.Equal("bar", mi.Headers["foo"]);
            Assert.Null(mi.Error);
            
            json = ReadDataFile("MessageInfo2.json");
            mi = new MessageInfo(json);
            Assert.Equal("subject2", mi.Subject);
            Assert.Equal(2U, mi.Sequence);
            Assert.Null(mi.Data);
            Assert.Null(mi.Headers);
            Assert.Null(mi.Error);

            json = ReadDataFile("MessageInfo3.json");
            mi = new MessageInfo(json);
            Assert.NotNull(mi.Error);
            Assert.True(mi.HasError);
            Assert.Equal(404, mi.ErrorCode);
            Assert.Equal("no message found", mi.ErrorDescription);
        }
    }
}
