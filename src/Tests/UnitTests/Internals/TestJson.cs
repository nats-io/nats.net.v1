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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests.JetStream
{
    public class TestJson : TestBase
    {
        private readonly ITestOutputHelper outputHelper;

        public TestJson(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
        }
        
        [Fact]
        public void TestSimpleMessageBody()
        {
            byte[] bytes = JsonUtils.SimpleMessageBody("ulong", 18446744073709551614ul);
            Assert.Equal("{\"ulong\":18446744073709551614}", Encoding.ASCII.GetString(bytes));
        }
        
        [Fact]
        public void TestTypes()
        {
            string json = ReadDataFile("TestJson.json");
            JSONNode jsonNode = JSON.Parse(json);
            Assert.Equal(int.MinValue, jsonNode["imin"].AsInt);
            Assert.Equal(-1, jsonNode["iminusone"].AsInt);
            Assert.Equal(0, jsonNode["izero"].AsInt);
            Assert.Equal(1, jsonNode["ione"].AsInt);
            Assert.Equal(int.MaxValue, jsonNode["imax"].AsInt);
        
            Assert.Equal(long.MinValue, jsonNode["lmin"].AsLong);
            Assert.Equal(-1, jsonNode["lminusone"].AsLong);
            Assert.Equal(0, jsonNode["lzero"].AsLong);
            Assert.Equal(1, jsonNode["lone"].AsLong);
            Assert.Equal(long.MaxValue, jsonNode["lmax"].AsLong);
        
            Assert.Equal(0ul, jsonNode["uzero"].AsUlong);
            Assert.Equal(1ul, jsonNode["uone"].AsUlong);
            Assert.Equal(ulong.MaxValue - 1, jsonNode["unotmax"].AsUlong);
            Assert.Equal(ulong.MaxValue, jsonNode["umax"].AsUlong);

            Assert.True(jsonNode["btrue"].AsBool);
            Assert.False(jsonNode["bfalse"].AsBool);
        }

        [Fact]
        public void TestReadPurgeResponseJson()
        {
            string json = ReadDataFile("PurgeResponse.json");
            PurgeResponse pr = new PurgeResponse(json, false);
            Assert.True(pr.Success);
            Assert.Equal(5ul, pr.Purged);
        }
    }
}
