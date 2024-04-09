// Copyright 2019-2024 The NATS Authors
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

using System.IO;
using System.Security;

namespace NATS.Client
{
    public class JWTHandlerUtils
    {
        public static string LoadUser(string text)
        {
            StringReader reader = null;
            try
            {
                reader = new StringReader(text);
                for (string line = reader.ReadLine(); line != null; line = reader.ReadLine())
                {
                    if (line.Contains("-----BEGIN NATS USER JWT-----"))
                    {
                        return reader.ReadLine();
                    }
                }

                return null;
            }
            finally
            {
                reader?.Dispose();
            }
        }
        
        public static NkeyPair LoadNkeyPair(string nkeySeed)
        {
            StringReader reader = null;
            try
            {
                // if it's a nk file, it only has the nkey
                if (nkeySeed.StartsWith("SU"))
                {
                    return Nkeys.FromSeed(nkeySeed);
                }

                // otherwise assume it's a creds file.
                reader = new StringReader(nkeySeed);
                for (string line = reader.ReadLine(); line != null; line = reader.ReadLine())
                {
                    if (line.Contains("-----BEGIN USER NKEY SEED-----"))
                    {
                        return Nkeys.FromSeed(reader.ReadLine());
                    }
                }

                return null;
            }
            finally
            {
                reader?.Dispose();
            }
        }
    }
}
