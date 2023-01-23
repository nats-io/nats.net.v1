// Copyright 2022 The NATS Authors
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
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using NATS.Client.Service;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;

namespace IntegrationTestsInternal
{
    public class TestService : TestSuite<ServiceSuiteContext>
    {
        public TestService(ServiceSuiteContext context) : base(context) {}

        [Fact]
        public void TestHandlerException() {
        }

        [Fact]
        public void TestServiceMessage() {
        }

        [Fact]
        public void TestServiceBuilderConstruction()
        {
        }
    }
}