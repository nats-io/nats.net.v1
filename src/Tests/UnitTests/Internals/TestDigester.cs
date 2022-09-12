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
using NATS.Client.Internals;
using Xunit;

namespace UnitTests.Internals
{
    public class TestDigester : TestBase
    {
        [Fact]
        public void TestDigesterWorks()
        {
            string s = ReadDataFile("digester_test_bytes_000100.txt");
            Digester d = new Digester();
            d.AppendData(s);
            Assert.Equal("IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=", d.GetDigestValue());

            Digester d2 = new Digester();
            d2.AppendData(s);
            Assert.Equal("IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=", d.GetDigestValue());

            Assert.True(d.DigestEntriesMatch(d2.GetDigestEntry()));
            Assert.True(d2.DigestEntriesMatch(d.GetDigestEntry()));
            Assert.True(d2.DigestEntriesMatch("SHA-256=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8="));
            Assert.False(d2.DigestEntriesMatch("SHA-999=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8="));
            
            s = ReadDataFile("digester_test_bytes_001000.txt");
            d = new Digester();
            d.AppendData(s);
            Assert.Equal("DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=", d.GetDigestValue());

            s = ReadDataFile("digester_test_bytes_010000.txt");
            d = new Digester();
            d.AppendData(s);
            Assert.Equal("RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=", d.GetDigestValue());

            s = ReadDataFile("digester_test_bytes_100000.txt");
            d = new Digester();
            d.AppendData(s);
            Assert.Equal("yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=", d.GetDigestValue());

            Assert.Throws<InvalidOperationException>(() => d.AppendData(s));
        }
    }
}
