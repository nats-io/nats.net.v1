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

using System;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    internal class TestingApiResponse : ApiResponse {
        internal TestingApiResponse(string json) : base(json) {}
    }
    
    public class TestApiResponse : TestBase
    {
        [Fact]
        public void DoesNotHaveAnError() {
            string json = ReadDataFile("ConsumerInfo.json");
            TestingApiResponse jsApiResp = new TestingApiResponse(json);
            Assert.False(jsApiResp.HasError);
            Assert.Null(jsApiResp.Error);
        }

        [Fact]
        public void MiscErrorResponsesAreUnderstood() {
            string text = ReadDataFile("ErrorResponses.json.txt");
            String[] jsons = text.Split('~');

            TestingApiResponse jsApiResp = new TestingApiResponse(jsons[0]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal("code_and_desc_response", jsApiResp.Type);
            Assert.Equal(500, jsApiResp.ErrorCode);
            Assert.Equal("the description", jsApiResp.ErrorDescription);
            Assert.Equal("the description (500)", jsApiResp.Error.ToString());
            NATSJetStreamException jsApiEx = new NATSJetStreamException(jsApiResp);
            Assert.Equal(500, jsApiEx.ErrorCode);
            Assert.Equal("the description (500)", jsApiEx.ErrorDescription);

            jsApiResp = new TestingApiResponse(jsons[1]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal("zero_and_desc_response", jsApiResp.Type);
            Assert.Equal(0, jsApiResp.ErrorCode);
            Assert.Equal("the description", jsApiResp.ErrorDescription);
            Assert.Equal("the description (0)", jsApiResp.Error.ToString());
            jsApiEx = new NATSJetStreamException(jsApiResp);
            Assert.Equal(0, jsApiEx.ErrorCode);
            Assert.Equal("the description (0)", jsApiEx.ErrorDescription);

            jsApiResp = new TestingApiResponse(jsons[2]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal("non_zero_code_only_response", jsApiResp.Type);
            Assert.Equal(500, jsApiResp.ErrorCode);
            Assert.Equal("Unknown JetStream Error (500)", jsApiResp.Error.ToString());
            jsApiEx = new NATSJetStreamException(jsApiResp);
            Assert.Equal(500, jsApiEx.ErrorCode);

            jsApiResp = new TestingApiResponse(jsons[3]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal("no_code_response", jsApiResp.Type);
            Assert.Equal(Error.NOT_SET, jsApiResp.ErrorCode);
            Assert.Equal("no code", jsApiResp.ErrorDescription);
            Assert.Equal("no code", jsApiResp.Error.ToString());
            jsApiEx = new NATSJetStreamException(jsApiResp);
            Assert.Equal(-1, jsApiEx.ErrorCode);
            Assert.Equal("no code", jsApiEx.ErrorDescription);

            jsApiResp = new TestingApiResponse(jsons[4]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal("empty_response", jsApiResp.Type);
            Assert.Equal(Error.NOT_SET, jsApiResp.ErrorCode);
            Assert.Empty(jsApiResp.ErrorDescription);
            jsApiEx = new NATSJetStreamException(jsApiResp);
            Assert.Equal(-1, jsApiEx.ErrorCode);
            Assert.Equal(-1, jsApiEx.ApiErrorCode);
            Assert.Empty(jsApiEx.ErrorDescription);

            jsApiResp = new TestingApiResponse(jsons[5]);
            Assert.True(jsApiResp.HasError);
            Assert.Equal(ApiResponse.NoType, jsApiResp.Type);
            Assert.Equal(ApiResponse.NoType, jsApiResp.Type); // coverage!
        }
    }
}
