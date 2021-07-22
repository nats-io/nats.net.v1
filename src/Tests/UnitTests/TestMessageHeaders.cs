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
using NATS.Client.Internals;
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
            byte[] hb = Encoding.ASCII.GetBytes("NATS/1.0\r\nfoo:bar\r\nbaz:bam\r\n\r\n");

            var hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(2, hsr.Header.Count);
            Assert.True(hsr.Status == null);
            Assert.Equal("bar", hsr.Header["foo"]);
            Assert.Equal("bam", hsr.Header["baz"]);

            // Test inline status and description which will come from the server.
            hb = Encoding.ASCII.GetBytes("NATS/1.0 503 an error\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(0, hsr.Header.Count);
            Assert.NotNull(hsr.Status);;
            Assert.Equal(NatsConstants.NoRespondersCode, hsr.Status.Code);
            Assert.Equal("an error", hsr.Status.Message);

            // Test inline status and description which will come from the server.
            hb = Encoding.ASCII.GetBytes("NATS/1.0    503    an error   \r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(0, hsr.Header.Count);
            Assert.NotNull(hsr.Status);;
            Assert.Equal(NatsConstants.NoRespondersCode, hsr.Status.Code);
            Assert.Equal("an error", hsr.Status.Message);

            // Test inline status and description which will come from the server.
            hb = Encoding.ASCII.GetBytes("NATS/1.0 404 Not Found\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(0, hsr.Header.Count);
            Assert.NotNull(hsr.Status);;
            Assert.Equal(NatsConstants.NotFoundCode, hsr.Status.Code);
            Assert.Equal("Not Found", hsr.Status.Message);

            // test quoted strings
            hb = Encoding.ASCII.GetBytes("NATS/1.0\r\nfoo:\"string:with:quotes\"\r\nbaz:no:quotes\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(2, hsr.Header.Count);
            Assert.True(hsr.Status == null);
            Assert.Equal("\"string:with:quotes\"", hsr.Header["foo"]);
            Assert.Equal("no:quotes", hsr.Header["baz"]);

            // Test unquoted strings.  Technically not to spec, but
            // support anyhow.
            hb = Encoding.ASCII.GetBytes("NATS/1.0\r\nfoo::::\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(1, hsr.Header.Count);
            Assert.True(hsr.Status == null);
            Assert.Equal(":::", hsr.Header["foo"]);

            // Test inline status which will come from the server.
            hb = Encoding.ASCII.GetBytes("NATS/1.0 503\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.Equal(0, hsr.Header.Count);
            Assert.NotNull(hsr.Status);;
            Assert.Equal(NatsConstants.NoRespondersCode, hsr.Status.Code);

            // Test inline status with kv pair.
            hb = Encoding.ASCII.GetBytes("NATS/1.0 123\r\nfoo:bar\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.True(hsr.Header.Count == 1);
            Assert.NotNull(hsr.Status);;
            Assert.Equal("bar", hsr.Header["foo"]);
            Assert.Equal(123, hsr.Status.Code);
            Assert.Equal("Server Status Message: 123", hsr.Status.Message);
                
            // Test inline status with kv pair.
            hb = Encoding.ASCII.GetBytes("NATS/1.0 503 hello\r\nfoo:bar\r\n\r\n");
            hsr = new HeaderStatusReader(hb, hb.Length);
            Assert.True(hsr.Header.Count == 1);
            Assert.NotNull(hsr.Status);;
            Assert.Equal("bar", hsr.Header["foo"]);
            Assert.Equal(NatsConstants.NoRespondersCode, hsr.Status.Code);
            Assert.Equal("hello", hsr.Status.Message);
        }

        [Fact]
        public void TestHeaderSerialization()
        {
            string headers = "NATS/1.0\r\nfoo:bar\r\n\r\n";
            byte[] headerBytes = Encoding.ASCII.GetBytes(headers);

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
            var hsr = new HeaderStatusReader(bytes, bytes.Length);
            Assert.Equal("bar", hsr.Header["foo"]);

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
            hsr = new HeaderStatusReader(bytes, bytes.Length);
            Assert.Equal("bar", hsr.Header["foo"]);
            Assert.Equal("", hsr.Header["Null-Value"]);
            Assert.Equal("", hsr.Header["Empty-Value"]);
            Assert.Equal(lv, hsr.Header["LargeValue"]);
        }

        [Fact]
        public void TestHeaderCopyConstructor()
        {
            var mh = new MsgHeader();
            mh["foo"] = "bar";

            var mh2 = new MsgHeader(mh);
            Assert.Equal("bar", mh2["foo"]);
        }

        [Fact]
        public void TestHeaderMultiValueSerialization()
        {
            string headers = "NATS/1.0\r\nfoo:bar\r\nfoo:baz,comma\r\n\r\n";
            byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
            var mh = new HeaderStatusReader(headerBytes, headerBytes.Length).Header;

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
            var hsr = new HeaderStatusReader(bytes, bytes.Length);
            Assert.Equal("bar,baz,comma", hsr.Header["foo"]);

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
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(null, 1));
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(new byte[16], 17));
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(null, 1));
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(new byte[16], 0));
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(new byte[16], -1));

            byte[] b = Encoding.ASCII.GetBytes("GARBAGE");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // No headers
            b = Encoding.ASCII.GetBytes("NATS/1.0");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // No headers
            b = Encoding.ASCII.GetBytes("NATS/1.0\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // Missing last \r\n
            b = Encoding.ASCII.GetBytes("NATS/1.0\r\nk1:v1\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // invalid headers
            b = Encoding.ASCII.GetBytes("NATS/1.0\r\ngarbage\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // missing key
            b = Encoding.ASCII.GetBytes("NATS/1.0\r\n:value\r\n\r\n");
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));

            // test invalid characters
            var mh = new MsgHeader();
            Assert.Throws<ArgumentException>(() => mh["k\r\ney"] = "value");
            Assert.Throws<ArgumentException>(() => mh["key"] = "val\r\nue");
            Assert.Throws<ArgumentException>(() => mh["foo:bar"] = "value");
            Assert.Throws<ArgumentException>(() => mh["foo"] = "value\f");
            Assert.Throws<ArgumentException>(() => mh["foo\f"] = "value");

            // test constructor with invalid assignment
            Assert.Throws<ArgumentException>(() => new MsgHeader() { ["foo:bar"] = "baz" });
            
            // more copied from java
            shouldNATSInvalidHeaderException("");
            shouldNATSInvalidHeaderException("NATS/0.0");
            shouldNATSInvalidHeaderException("NATS/1.0");
            shouldNATSInvalidHeaderException("NATS/1.0 \r\n");
            shouldNATSInvalidHeaderException("NATS/1.0X\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0 \r\n\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\n\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0 503\r");
            shouldNATSInvalidHeaderException("NATS/1.0 503\n");
            shouldNATSInvalidHeaderException("NATS/1.0 FiveOhThree\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\n\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\n\r\n\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\nk1:v1");
            shouldNATSInvalidHeaderException("NATS/1.0\r\nk1:v1\r\n");
            shouldNATSInvalidHeaderException("NATS/1.0\r\nk1:v1\r\r\n");
        }

        private void shouldNATSInvalidHeaderException(string s)
        {
            byte[] b = Encoding.ASCII.GetBytes(s);
            Assert.Throws<NATSInvalidHeaderException>(() => new HeaderStatusReader(b, b.Length));
        }

        [Fact]
        public void TestToken()
        {
            byte[] serialized1 = Encoding.ASCII.GetBytes("notspaceorcrlf");
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized1, serialized1.Length, 0, TokenType.Word));
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized1, serialized1.Length, 0, TokenType.Key));
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized1, serialized1.Length, 0, TokenType.Space));
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized1, serialized1.Length, 0, TokenType.Crlf));

            byte[] serialized2 = Encoding.ASCII.GetBytes("\r");
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized2, serialized2.Length, 0, TokenType.Crlf));

            byte[] serialized3 = Encoding.ASCII.GetBytes("\rnotlf");
            Assert.Throws<NATSInvalidHeaderException>(() => new Token(serialized3, serialized3.Length, 0, TokenType.Crlf));

            Token t = new Token(Encoding.ASCII.GetBytes("k1:v1\r\n\r\n"), 9, 0, TokenType.Key);
            t.MustBe(TokenType.Key);
            Assert.Throws<NATSInvalidHeaderException>(() => t.MustBe(TokenType.Crlf));
        }

        [Fact]
        public void TestTokenSamePoint()
        {
            byte[] serialized1 = Encoding.ASCII.GetBytes("\r\n");
            Token t1 = new Token(serialized1, serialized1.Length, 0, TokenType.Space);
            // equals
            Token t1Same = new Token(serialized1, serialized1.Length, 0, TokenType.Space);
            Assert.True(t1.SamePoint(t1Same));

            // same start, same end, different type
            byte[] notSame = Encoding.ASCII.GetBytes("x\r\n");
            Token tNotSame = new Token(notSame, notSame.Length, 0, TokenType.Text);
            Assert.False(t1.SamePoint(tNotSame));

            // same start, different end, same type
            notSame = Encoding.ASCII.GetBytes("  \r\n");
            tNotSame = new Token(notSame, notSame.Length, 0, TokenType.Space);
            Assert.False(t1.SamePoint(tNotSame));

            // different start
            notSame = Encoding.ASCII.GetBytes("x  \r\n");
            tNotSame = new Token(notSame, notSame.Length, 1, TokenType.Space);
            Assert.False(t1.SamePoint(tNotSame));
        }
    }
}