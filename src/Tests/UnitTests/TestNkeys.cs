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
            byte[] b = Nkeys.DecodeSeed( Nkeys.Encode(20 << 3, true, a));
            Assert.Equal(a, b);
            
            Random rnd = new Random();
            rnd.NextBytes(a);
            b = Nkeys.DecodeSeed( Nkeys.Encode(20 << 3, true, a));
            Assert.Equal(a, b);
        }
        
        [Fact]
        public void TestNKEYCreateUserSeed()
        {
            string user = Nkeys.CreateUserSeed();
            Assert.NotEmpty(user);
            Assert.False(user.EndsWith("=", StringComparison.Ordinal));
            Assert.NotNull(Nkeys.FromSeed(user));
            string pk = Nkeys.PublicKeyFromSeed(user);
            Assert.Equal('U', pk[0]);
        }

        [Fact]
        public void TestNKEYCreateAccountSeed()
        {
            string acc = Nkeys.CreateAccountSeed();
            Assert.NotEmpty(acc);
            Assert.False(acc.EndsWith("=", StringComparison.Ordinal));
            Assert.NotNull(Nkeys.FromSeed(acc));
            string pk = Nkeys.PublicKeyFromSeed(acc);
            Assert.Equal('A', pk[0]);
        }

        [Fact]
        public void TestNKEYCreateOperatorSeed()
        {
            string op = Nkeys.CreateOperatorSeed();
            Assert.NotEmpty(op);
            Assert.False(op.EndsWith("=", StringComparison.Ordinal));
            Assert.NotNull(Nkeys.FromSeed(op));
            string pk = Nkeys.PublicKeyFromSeed(op);
            Assert.Equal('O', pk[0]);
        }

        [Fact]
        public void TestNKEYPublicKeyFromSeed()
        {
            // using nsc generated seeds for testing
            string pk = Nkeys.PublicKeyFromSeed("SOAELH6NJCEK4HST5644G4HK7TOAFZGRRJHNM4EUKUY7PPNDLIKO5IH4JM");
            Assert.Equal("ODPWIBQJVIQ42462QAFI2RKJC4RZHCQSIVPRDDHWFCJAP52NRZK6Z2YC", pk);

            pk = Nkeys.PublicKeyFromSeed("SAANWFZ3JINNPERWT3ALE45U7GYT2ZDW6GJUIVPDKUF6GKAX6AISZJMAS4");
            Assert.Equal("AATEJXG7UX4HFJ6ZPRTP22P6OYZER36YYD3GVBOVW7QHLU32P4QFFTZJ", pk);

            pk = Nkeys.PublicKeyFromSeed("SUAGDLNBWI2SGHDRYBHD63NH5FGZSVJUW2J7GAJZXWANQFLDW6G5SXZESU");
            Assert.Equal("UBICBTHDKQRB4LIYA6BMIJ7EA2G7YS7FIWMMVKZJE6M3HS5IVCOLKDY2", pk);
        }
    }
    
#pragma warning restore CS0618
}