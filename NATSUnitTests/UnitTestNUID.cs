// Copyright 2015-2018 The NATS Authors
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
using System.Diagnostics;
using NATS.Client;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestNUID
    {
        [Fact]
        public void TestGlobalNUID()
        {
            NUID n = NUID.Instance;
            Assert.NotNull(n);
            Assert.NotNull(n.Pre);
            Assert.NotEqual(0, n.Seq);
        }

        [Fact]
        public void TestNUIDRollover()
        {
            NUID gnuid = NUID.Instance;
            gnuid.Seq = NUID.MAXSEQ;

            byte[] prefix = new byte[gnuid.Pre.Length];
            Array.Copy(gnuid.Pre, prefix, gnuid.Pre.Length);

            string nextvalue = gnuid.Next;

            bool areEqual = true;
            for (int i = 0; i < gnuid.Pre.Length; i++)
            {
                if (prefix[i] != gnuid.Pre[i])
                    areEqual = false;
            }

            Assert.False(areEqual);
        }

        [Fact]
        public void TestNUIDLen()
        {
            string nuid = new NUID().Next;
            Assert.Equal(nuid.Length, NUID.LENGTH);
        }

        static void printElapsedTime(long operations, Stopwatch sw)
        {
            double nanoseconds = ((double)sw.ElapsedTicks /
                ((double)Stopwatch.Frequency) * (double)1000000000);
            System.Console.WriteLine("Nuid Test:  Performed {0} operations in {1} nanos.  {2} ns/op",
                operations, nanoseconds, (long)(nanoseconds / (double)operations));
        }

        private void runNUIDSpeedTest(NUID n)
        {
            long count = 10000000;

            Stopwatch sw = Stopwatch.StartNew();

            for (long i = 0; i < count; i++)
            {
                string nuid = n.Next;
            }

            sw.Stop();

            printElapsedTime(count, sw);
        }

        [Fact]
        public void TestNUIDSpeed()
        {
            runNUIDSpeedTest(NUID.Instance);
        }

        [Fact]
        public void TestGlobalNUIDSpeed()
        {
            runNUIDSpeedTest(new NUID());
        }

        [Fact]
        public void TestNuidBasicUniqueess()
        {
            int count = 1000000;
            IDictionary<string, bool> m = new Dictionary<string, bool>(count);

            for (int i = 0; i < count; i++)
            {
                String n = NUID.NextGlobal;
                Assert.False(m.ContainsKey(n), string.Format("Duplicate NUID found: {0}", n));

                m.Add(n, true);
            }
        }

    } // class

} // namespace

