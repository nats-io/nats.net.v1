// Copyright 2019 The NATS Authors
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
    public class DefaultUserJWTHandler
    {
        private string jwtFile;
        private string credsFile;

        /// <summary>
        /// Gets the JWT file.
        /// </summary>
        public string JwtFile => jwtFile;

        /// <summary>
        /// Gets the credentials files.
        /// </summary>
        public string CredsFile => credsFile;

        /// <summary>
        /// Creates the default user jwt handler.
        /// </summary>
        /// <param name="jwtFilePath">Full path the to user JWT</param>
        /// <param name="credsFilePath">Full path to the user private credentials file.
        /// May be the same as the jwt file if they are chained.</param>
        public DefaultUserJWTHandler(string jwtFilePath, string credsFilePath)
        {
            jwtFile = jwtFilePath;
            credsFile = credsFilePath;
        }

        /// <summary>
        /// Gets a user JWT from a user JWT or chained credentials file.
        /// </summary>
        /// <param name="path">Full path to the JWT or cred file.</param>
        /// <returns>The encoded JWT</returns>
        public static string LoadUserFromFile(string path)
        {
            string text = File.ReadAllText(path).Trim();
            if (string.IsNullOrEmpty(text))
            {
                throw new NATSException("Credentials file is empty");
            }
            string user = JWTHandlerUtils.LoadUser(text);
            if (user == null)
            {
                throw new NATSException("Credentials file does not contain a JWT");
            }
            return user;
        }
        
        /// <summary>
        /// Generates a NATS Ed25519 keypair, used to sign server nonces, from a 
        /// private credentials file.
        /// </summary>
        /// <param name="path">The credentials file, could be a "*.nk" or "*.creds" file.</param>
        /// <returns>A NATS Ed25519 KeyPair</returns>
        public static NkeyPair LoadNkeyPairFromSeedFile(string path)
        {
            StringReader reader = null;
            try
            {
                string text = File.ReadAllText(path).Trim();
                if (string.IsNullOrEmpty(text))
                {
                    throw new NATSException("Credentials file is empty");
                }
                NkeyPair kp = JWTHandlerUtils.LoadNkeyPair(text);
                if (kp == null)
                {
                    throw new NATSException("Seed not found in credentials file.");
                }
                return kp;
            }
            finally
            {
                reader?.Dispose();
            }
        }

        /// <summary>
        /// The default User JWT Event Handler.
        /// </summary>
        /// <param name="sender">Usually the connection.</param>
        /// <param name="args">Arguments</param>
        public void DefaultUserJWTEventHandler(object sender, UserJWTEventArgs args)
        {
            args.JWT = LoadUserFromFile(jwtFile);
        }

        /// <summary>
        /// Utility method to signs the UserSignatureEventArgs server nonce from 
        /// a private credentials file.
        /// </summary>
        /// <param name="credsFile">A file with the private Nkey</param>
        /// <param name="args">Arguments</param>
        public static void SignNonceFromFile(string credsFile, UserSignatureEventArgs args)
        {
            var kp = LoadNkeyPairFromSeedFile(credsFile);
            args.SignedNonce = kp.Sign(args.ServerNonce);
            kp.Wipe();
        }

        /// <summary>
        /// The default User Signature event handler.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        public void DefaultUserSignatureHandler(object sender, UserSignatureEventArgs args)
        {
            SignNonceFromFile(credsFile, args);
        }
    }
}
