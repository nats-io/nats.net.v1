// Copyright 2015-2019 The NATS Authors
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
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace IntegrationTests
{
    /// <summary>
    /// Custom attribute for allowing retries of certain failures that
    /// sporadically occurs at CI server.
    /// </summary>
    [XunitTestCaseDiscoverer("IntegrationTests.NatsTestCaseDiscoverer", "IntegrationTests")]
    public class NatsFactAttribute : FactAttribute
    {
    }

    public class NatsTestCaseDiscoverer : IXunitTestCaseDiscoverer
    {
        private readonly IMessageSink diagnosticMessageSink;

        public NatsTestCaseDiscoverer(IMessageSink diagnosticMessageSink)
        {
            this.diagnosticMessageSink = diagnosticMessageSink;
        }

        public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute)
        {
            yield return new NatsTestCase(diagnosticMessageSink, discoveryOptions.MethodDisplayOrDefault(), discoveryOptions.MethodDisplayOptionsOrDefault(), testMethod);
        }
    }

    public class NatsTestCase : XunitTestCase
    {
        private const int MaxNumberOfExecutions = 2;
        
        private static readonly HashSet<string> ExceptionsToRetry = new HashSet<string>(
            new[]
            {
                typeof(NATSNoServersException).FullName
            }, StringComparer.OrdinalIgnoreCase);

        [EditorBrowsable(EditorBrowsableState.Never)]
        [Obsolete("Do not use. Only for Xunit plumbing.", true)]
        public NatsTestCase()
        {
        }

        public NatsTestCase(
            IMessageSink diagnosticMessageSink,
            TestMethodDisplay defaultMethodDisplay,
            TestMethodDisplayOptions defaultMethodDisplayOptions,
            ITestMethod testMethod,
            object[] testMethodArguments = null)
            : base(diagnosticMessageSink, defaultMethodDisplay, defaultMethodDisplayOptions, testMethod, testMethodArguments)
        {
        }

        public override async Task<RunSummary> RunAsync(
            IMessageSink diagnosticMessageSink,
            IMessageBus messageBus,
            object[] constructorArguments,
            ExceptionAggregator aggregator,
            CancellationTokenSource cancellationTokenSource)
        {
            var interceptingMessageBus = new FailureInterceptingMessageBus(messageBus);
            var runner = new XunitTestCaseRunner(this, DisplayName, SkipReason, constructorArguments, TestMethodArguments, interceptingMessageBus, aggregator, cancellationTokenSource);

            var executionCount = 0;

            while (true)
            {
                var r = await runner.RunAsync();
                if (r.Failed == 0 || cancellationTokenSource.IsCancellationRequested)
                    return r;

                var retryableFailures = interceptingMessageBus.GetInterceptedFailures().Where(ShouldRetryFailure).ToArray();
                if (!retryableFailures.Any())
                    return r;

                interceptingMessageBus.ClearInterceptedFailures();

                var retry = ++executionCount < MaxNumberOfExecutions;
                if (!retry)
                {
                    foreach (var failure in retryableFailures)
                        messageBus.QueueMessage(failure);

                    return r;
                }

                foreach (var failure in retryableFailures)
                    messageBus.QueueMessage(new TestSkipped(failure.Test, "Retry"));
            }
        }

        private static bool ShouldRetryFailure(TestFailed failure)
            => failure.ExceptionTypes.Any(ExceptionsToRetry.Contains);

        private class FailureInterceptingMessageBus : IMessageBus
        {
            private readonly IMessageBus messageBus;

            private readonly List<TestFailed> interceptedFailures = new List<TestFailed>();

            public FailureInterceptingMessageBus(IMessageBus mb)
            {
                messageBus = mb;
            }

            public void Dispose() => messageBus.Dispose();

            public void ClearInterceptedFailures()
                => interceptedFailures.Clear();

            public IEnumerable<TestFailed> GetInterceptedFailures()
                => interceptedFailures;

            public bool QueueMessage(IMessageSinkMessage message)
            {
                if (message is TestFailed failure)
                {
                    interceptedFailures.Add(failure);
                    return true;
                }
                
                return messageBus.QueueMessage(message);
            }
        }
    }
}