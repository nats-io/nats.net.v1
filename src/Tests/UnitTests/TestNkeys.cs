// Copyright 2021-2021 The NATS Authors
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
using NATS.Client;
using Xunit;

namespace UnitTests
{
#pragma warning disable CS0618
    public class TestNkeys
    { 
        [Fact]
        public void TestNKEYEncodeDecode()
        {
            byte[] a = new Byte[32];
            byte[] b = Nkeys.DecodeSeed( Nkeys.EncodeSeed(20 << 3, a));
            Assert.Equal(a, b);
            
            Random rnd = new Random();
            rnd.NextBytes(a);
            b = Nkeys.DecodeSeed( Nkeys.EncodeSeed(20 << 3, a));
            Assert.Equal(a, b);
        }
        
        [Fact]
        public void TestNKEYCreateUserSeed()
        {
            string user = Nkeys.CreateUserSeed();
            Assert.NotEmpty(user);
            Assert.NotNull(Nkeys.FromSeed(user));
        }

        [Fact]
        public void TestNKEYCreateAccountSeed()
        {
            string acc = Nkeys.CreateAccountSeed();
            Assert.NotEmpty(acc);
            Assert.NotNull(Nkeys.FromSeed(acc));
        }

        [Fact]
        public void TestNKEYCreateOperatorSeed()
        {
            string op = Nkeys.CreateOperatorSeed();
            Assert.NotEmpty(op);
            Assert.NotNull(Nkeys.FromSeed(op));
        }
    }
    
#pragma warning restore CS0618
}