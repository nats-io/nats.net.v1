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
using System.Security.Cryptography;
using System.Text;

namespace NATS.Client.Internals
{
    public class Digester
    {
        private IncrementalHash hasher;
        private string digest;
        private string entry;

        public Digester()
        {
            hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        }
        
        public void AppendData(string s)
        {
            if (digest != null)
            {
                throw new InvalidOperationException("Digest has already been prepared.");
            }
            hasher.AppendData(Encoding.UTF8.GetBytes(s));
        }
        
        public void AppendData(byte[] data)
        {
            if (digest != null)
            {
                throw new InvalidOperationException("Digest has already been prepared.");
            }
            hasher.AppendData(data);
        }
        
        private void _prepareDigest()
        {
            if (digest == null)
            {
                byte[] hash = hasher.GetHashAndReset();
                digest = Convert.ToBase64String(hash).Replace('+', '-').Replace('/', '_');
                entry = "SHA-256=" + digest;
            }
        }

        public string GetDigestValue()
        {
            _prepareDigest();
            return digest;
        }

        public string GetDigestEntry()
        {
            _prepareDigest();
            return entry;
        }

        public bool DigestEntriesMatch(string thatEntry)
        {
            try
            {
                string thisEntry = GetDigestEntry();
                int at = thisEntry.IndexOf("=", StringComparison.Ordinal);
                return thisEntry.Substring(0, at).ToUpper().Equals(thatEntry.Substring(0, at).ToUpper()) 
                       && thisEntry.Substring(at).Equals(thatEntry.Substring(at));
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}