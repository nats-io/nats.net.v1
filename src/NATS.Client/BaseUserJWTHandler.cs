﻿// Copyright 2019-2024 The NATS Authors
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
    /// <summary>
    /// This class is contains the default handlers for the
    /// <see cref="Options.UserJWTEventHandler"/> and the 
    /// <see cref="Options.UserSignatureEventHandler"/>.  This class is
    /// not normally used directly, but is provided to extend or use for
    /// utility methods to read a private seed or user JWT.
    /// </summary>
    public class BaseUserJWTHandler
    {
        protected static string _loadUser(string text)
        {
            StringReader reader = null;
            try
            {
                reader = new StringReader(text.ToString());
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
        
        protected static NkeyPair _loadNkeyPair(string secure)
        {
            StringReader reader = null;
            try
            {
                string text = secure.ToString();
                
                // if it's a nk file, it only has the nkey
                if (text.StartsWith("SU"))
                {
                    return Nkeys.FromSeed(text);
                }

                // otherwise assume it's a creds file.
                reader = new StringReader(text);
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
