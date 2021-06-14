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
using System.Collections.Generic;
using System.Text;
using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestMessageHeaders
    {

        [Fact]
        public void TestHeaderBasic()
        {
            var mh = new MsgHeader();
            mh["foo"] = "bar";
            Assert.Equal("bar", mh["foo"]);

            // check iteration
            foreach (string key in mh)
            {
                Assert.Equal("foo", key);
                Assert.Equal("bar", mh[key]);
            }

            // check iteration
            foreach (string key in mh)
            {
                Assert.Equal("foo", key);
                Assert.Equal("bar", mh[key]);
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

            // test quoted string
            mh["foo"] = "\"mystring:bar;foo:\"";
            Assert.Equal("\"mystring:bar;foo:\"", mh["foo"]);

        }

        [Fact]
        public void TestHeaderDeserialization()
        {
            byte[] hb = Encoding.UTF8.GetBytes($"NATS/1.0\r\nfoo:bar\r\nbaz:bam\r\n\r\n");

            var mh = new MsgHeader(hb, hb.Length);
            Assert.Equal("bar", mh["foo"]);
            Assert.Equal("bam", mh["baz"]);
            Assert.True(mh.Count == 2);

            // Test inline status and description which will come from the server.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0 503 an error\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 2);
            Assert.Equal(MsgHeader.NoResponders, mh[MsgHeader.Status]);
            Assert.Equal("an error", mh[MsgHeader.Description]);

            // Test inline status and description which will come from the server.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0    503    an error   \r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 2);
            Assert.Equal(MsgHeader.NoResponders, mh[MsgHeader.Status]);
            Assert.Equal("   an error   ", mh[MsgHeader.Description]);

            // Test inline status and description which will come from the server.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0 404 Not Found\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 2);
            Assert.Equal(MsgHeader.NotFound, mh[MsgHeader.Status]);
            Assert.Equal("Not Found", mh[MsgHeader.Description]);

            // test quoted strings
            hb = Encoding.UTF8.GetBytes($"NATS/1.0\r\nfoo:\"string:with:quotes\"\r\nbaz:no:quotes\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.Equal("\"string:with:quotes\"", mh["foo"]);
            Assert.Equal("no:quotes", mh["baz"]);

            // Test unquoted strings.  Technically not to spec, but
            // support anyhow.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0\r\nfoo::::\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.Equal(":::", mh["foo"]);

            // Test empty headers, which may come from the server.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 0);

            // Test inline status which will come from the server.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0 503\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 1);
            Assert.Equal(MsgHeader.NoResponders, mh[MsgHeader.Status]);

            // Test inline status with kv pair.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0 503\r\nfoo:bar\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 2);
            Assert.Equal(MsgHeader.NoResponders, mh[MsgHeader.Status]);
            Assert.Equal("bar", mh["foo"]);

            // Test inline status with kv pair.
            hb = Encoding.UTF8.GetBytes($"NATS/1.0 503 hello\r\nfoo:bar\r\n\r\n");
            mh = new MsgHeader(hb, hb.Length);
            Assert.True(mh.Count == 3);
            Assert.Equal(MsgHeader.NoResponders, mh[MsgHeader.Status]);
            Assert.Equal("hello", mh[MsgHeader.Description]);
            Assert.Equal("bar", mh["foo"]);
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
            Assert.Equal("bar", mh2["foo"]);

            // large header
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 20480; i++)
            {
                sb.Append("N");
            }
            string lv = sb.ToString();
            mh["LargeValue"] = lv;

            // test null and empty values
            mh["Null-Value"] = null;
            mh["Empty-Value"] = "";

            bytes = mh.ToByteArray();

            // now serialize back
            mh2 = new MsgHeader(bytes, bytes.Length);
            Assert.Equal("bar", mh2["foo"]);
            Assert.Equal("", mh2["Null-Value"]);
            Assert.Equal("", mh2["Empty-Value"]);
            Assert.Equal(lv, mh2["LargeValue"]);
        }

        [Fact]
        public void TestHeaderCopyConstructor()
        {
            var mh = new MsgHeader();
            mh["foo"] = "bar";

            var mh2 = new MsgHeader(mh);
            Assert.Equal("bar", mh2["foo"]);

            Assert.Throws<ArgumentNullException>(() => new MsgHeader(null));
            Assert.Throws<ArgumentException>(() => new MsgHeader(new MsgHeader()));
        }

        [Fact]
        public void TestHeaderMultiValueSerialization()
        {
            string headers = $"NATS/1.0\r\nfoo:bar\r\nfoo:baz,comma\r\n\r\n";
            byte[] headerBytes = Encoding.UTF8.GetBytes(headers);
            var mh = new MsgHeader(headerBytes, headerBytes.Length);

            byte[] bytes = mh.ToByteArray();
            Assert.True(bytes.Length == headerBytes.Length);
            for (int i = 0; i < bytes.Length; i++)
            {
                Assert.True(headerBytes[i] == bytes[i]);
            }
        }

        [Fact]
        public void TestHeaderMultiValues()
        {
            var mh = new MsgHeader();
            mh.Add("foo", "bar");
            mh.Add("foo", "baz,comma");

            // Test the GetValues API, don't make assumptions about order.
            string []values = mh.GetValues("foo");
            Assert.True(values.Length == 2);
            List<string> results = new List<string>(values);
            Assert.Contains("bar", results);
            Assert.Contains("baz,comma", results);

            byte[] bytes = mh.ToByteArray();
            var mh2 = new MsgHeader(bytes, bytes.Length);
            Assert.Equal("bar,baz,comma", mh2["foo"]);

            // test the API on a single value key
            mh = new MsgHeader();
            mh["foo"] = "bar";
            values = mh.GetValues("foo");
            Assert.True(values.Length == 1);
            Assert.Equal("bar", values[0]);
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

            // missing key
            b = Encoding.UTF8.GetBytes("NATS/1.0\r\n:value\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new MsgHeader(b, b.Length));

            // test invalid characters
            var mh = new MsgHeader();
            Assert.Throws<ArgumentException>(() => mh["k\r\ney"] = "value");
            Assert.Throws<ArgumentException>(() => mh["key"] = "val\r\nue");
            Assert.Throws<ArgumentException>(() => mh["foo:bar"] = "value");
            Assert.Throws<ArgumentException>(() => mh["foo"] = "value\f");
            Assert.Throws<ArgumentException>(() => mh["foo\f"] = "value");

            // test constructor with invalid assignment
            Assert.Throws<ArgumentException>(() => new MsgHeader() { ["foo:bar"] = "baz" });
        }
    }
}