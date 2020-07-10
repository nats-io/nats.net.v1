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
using System.Text;
using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestMessageHeaders
    {

        [Fact]
        public void TestHeadersBasic()
        {
            var mh = new MsgHeader();
            mh["foo"] = "bar";
            Assert.True(mh["foo"].Equals("bar"));

            // check iteration
            foreach (string key in mh)
            {
                Assert.True(key.Equals("foo"));
                Assert.True(mh[key].Equals("bar"));
            }

            mh["baz"] = "nnn";
            Assert.True(mh.Count == 2);

            // check that baz has been removed.
            mh.Remove("baz");
            Assert.True(mh.Count == 1);

            // reassign and check for null
            mh["foo"] = null;
            Assert.True(mh["foo"] == null);

            // test clearing it out
            mh.Clear();
            Assert.True(mh.Count == 0);
        }

        [Fact]
        public void TestHeaderDeserialization()
        {
            string headers = $"NATS/1.0\r\nfoo:bar\r\nbaz:bam\r\n\r\n";
            byte[] headerBytes = System.Text.Encoding.UTF8.GetBytes(headers);

            var mh = new MsgHeader(headerBytes, headerBytes.Length);
            Assert.True(mh["foo"].Equals("bar"));
            Assert.True(mh["baz"].Equals("bam"));
            Assert.True(mh.Count == 2);
        }

        [Fact]
        public void TestHeaderSerialization()
        {
            string headers = $"NATS/1.0\r\nfoo:bar\r\n\r\n";
            byte[] headerBytes = System.Text.Encoding.UTF8.GetBytes(headers);

            // can only test with one because order isn't guaranteed
            var mh = new MsgHeader();
            mh["foo"] = "bar";

            byte[] bytes = mh.ToByteArray();
            Assert.True(bytes.Length == headerBytes.Length);

            for (int i = 0; i < bytes.Length; i++)
            {
                Assert.True(headerBytes[i] == bytes[i]);
            }

            // now serialize back
            var mh2 = new MsgHeader(bytes, bytes.Length);
            Assert.True(mh2["foo"].Equals("bar"));
        }

        [Fact]
        public void TestHeaderCopyConstructor()
        {
            var mh = new MsgHeader();
            mh["foo"] = "bar";

            var mh2 = new MsgHeader(mh);
            Assert.True(mh2["foo"].Equals("bar"));
        }

        [Fact]
        public void TestHeaderMultiValues()
        {
            var mh = new MsgHeader();
            mh.Add("foo", "bar");
            mh.Add("foo", "baz");

            Assert.True(mh["foo"].Equals("bar,baz"));

            byte[] bytes = mh.ToByteArray();
            var mh2 = new MsgHeader(bytes, bytes.Length);
            Assert.True(mh2["foo"].Equals("bar,baz"));
        }

        [Fact]
        public void TestHeaderExceptions()
        {
            Assert.Throws<NATSException>(() => new MsgHeader(null, 1));
            Assert.Throws<NATSException>(() => new MsgHeader(new byte[16], 17));
            Assert.Throws<NATSException>(() => new MsgHeader(null, 1));
            Assert.Throws<NATSException>(() => new MsgHeader(new byte[16], 0));
            Assert.Throws<NATSException>(() => new MsgHeader(new byte[16], -1));

            Assert.Throws<ArgumentNullException>(() => new MsgHeader(null));

            byte[] b = Encoding.UTF8.GetBytes("GARBAGE");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // No headers
            b = Encoding.UTF8.GetBytes("NATS/1.0");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // No headers
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // Missing last \r\n
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\nk1:v1\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // invalid headers
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\ngarbage\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // missing value
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\nkey:\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // missing key
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\n:value\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));
        }
    }
}