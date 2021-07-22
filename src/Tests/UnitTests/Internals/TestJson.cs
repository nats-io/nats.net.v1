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
        private readonly ITestOutputHelper output;
        private readonly JSONNode testJsonNode;

        public TestJson(ITestOutputHelper outputHelper)
        {
            output = outputHelper;
            string json = ReadDataFile("TestJson.json");
            testJsonNode = JSON.Parse(json);
        }
        
        [Fact]
        public void TestTypes()
        {
            Assert.Equal(int.MinValue, testJsonNode["imin"].AsInt);
            Assert.Equal(-1, testJsonNode["iminusone"].AsInt);
            Assert.Equal(0, testJsonNode["izero"].AsInt);
            Assert.Equal(1, testJsonNode["ione"].AsInt);
            Assert.Equal(int.MaxValue, testJsonNode["imax"].AsInt);
            Assert.Equal(0, testJsonNode["notfound"].AsInt);
        
            Assert.Equal(long.MinValue, testJsonNode["lmin"].AsLong);
            Assert.Equal(-1, testJsonNode["lminusone"].AsLong);
            Assert.Equal(0, testJsonNode["lzero"].AsLong);
            Assert.Equal(1, testJsonNode["lone"].AsLong);
            Assert.Equal(long.MaxValue, testJsonNode["lmax"].AsLong);
            Assert.Equal(0, testJsonNode["notfound"].AsLong);
        
            Assert.Equal(0ul, testJsonNode["uzero"].AsUlong);
            Assert.Equal(1ul, testJsonNode["uone"].AsUlong);
            Assert.Equal(ulong.MaxValue - 1, testJsonNode["unotmax"].AsUlong);
            Assert.Equal(ulong.MaxValue, testJsonNode["umax"].AsUlong);
            Assert.Equal(0ul, testJsonNode["notfound"].AsUlong);

            Assert.True(testJsonNode["btrue"].AsBool);
            Assert.False(testJsonNode["bfalse"].AsBool);
        }

        [Fact]
        public void TestReadPurgeResponseJson()
        {
            string json = ReadDataFile("PurgeResponse.json");
            PurgeResponse pr = new PurgeResponse(json, false);
            Assert.True(pr.Success);
            Assert.Equal(5ul, pr.Purged);
        }
        
        [Fact]
        public void TestJsonUtilsSimpleMessageBody()
        {
            byte[] bytes = JsonUtils.SimpleMessageBody("ulong", 18446744073709551614ul);
            Assert.Equal("{\"ulong\":18446744073709551614}", Encoding.ASCII.GetString(bytes));
        }
        
        [Fact]
        public void TestJsonUtilsAsMethods()
        {
            Assert.Equal(-1, JsonUtils.AsIntOrMinus1(testJsonNode, "notfound"));
            Assert.Equal(int.MinValue, JsonUtils.AsIntOrMinus1(testJsonNode, "imin"));
            Assert.Equal(-1, JsonUtils.AsIntOrMinus1(testJsonNode, "iminusone"));
            Assert.Equal(0, JsonUtils.AsIntOrMinus1(testJsonNode, "izero"));
            Assert.Equal(1, JsonUtils.AsIntOrMinus1(testJsonNode, "ione"));
            Assert.Equal(int.MaxValue, JsonUtils.AsIntOrMinus1(testJsonNode, "imax"));
        
            Assert.Equal(-1, JsonUtils.AsLongOrMinus1(testJsonNode, "notfound"));
            Assert.Equal(long.MinValue, JsonUtils.AsLongOrMinus1(testJsonNode, "lmin"));
            Assert.Equal(-1, JsonUtils.AsLongOrMinus1(testJsonNode, "lminusone"));
            Assert.Equal(0, JsonUtils.AsLongOrMinus1(testJsonNode, "lzero"));
            Assert.Equal(1, JsonUtils.AsLongOrMinus1(testJsonNode, "lone"));
            Assert.Equal(long.MaxValue, JsonUtils.AsLongOrMinus1(testJsonNode, "lmax"));
        
            Assert.Equal(0ul, JsonUtils.AsUlongOrZero(testJsonNode, "notfound"));
            Assert.Equal(0ul, JsonUtils.AsUlongOrZero(testJsonNode, "uzero"));
            Assert.Equal(1ul, JsonUtils.AsUlongOrZero(testJsonNode, "uone"));
            Assert.Equal(ulong.MaxValue - 1, JsonUtils.AsUlongOrZero(testJsonNode, "unotmax"));
            Assert.Equal(ulong.MaxValue , JsonUtils.AsUlongOrZero(testJsonNode, "umax"));
        }
    }
}
