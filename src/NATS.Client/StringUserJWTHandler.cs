// Copyright 2024 The NATS Authors
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
    /// TODO
    /// </summary>
    public class StringUserJWTHandler
    {
        /// <summary>
        /// Gets the JWT file.
        /// </summary>
        public string UserJwt { get; }

        /// <summary>
        /// Gets the credentials files.
        /// </summary>
        public string NkeySeed { get; }

        /// <summary>
        /// Creates a static user jwt handler.
        /// </summary>
        /// <param name="credentialsText">The text containing the "-----BEGIN NATS USER JWT-----" block
        /// and the text containing the "-----BEGIN USER NKEY SEED-----" block</param>
        public StringUserJWTHandler(string credentialsText) : this(credentialsText, credentialsText) {}
        
        /// <summary>
        /// Creates a static user jwt handler.
        /// </summary>
        /// <param name="userJwt">The text containing the "-----BEGIN NATS USER JWT-----" block</param>
        /// <param name="nkeySeed">The text containing the "-----BEGIN USER NKEY SEED-----" block or the seed begining with "SU".
        /// May be the same as the jwt string if they are chained.</param>
        public StringUserJWTHandler(string userJwt, string nkeySeed)
        {
            UserJwt = JWTHandlerUtils.LoadUser(userJwt);
            if (UserJwt == null)
            {
                throw new NATSException("Credentials do not contain a JWT");
            }

            if (JWTHandlerUtils.LoadNkeyPair(nkeySeed) == null)
            {
                throw new NATSException("Seed not found.");
            }
            NkeySeed = nkeySeed;
        }

        /// <summary>
        /// The default User JWT Event Handler.
        /// </summary>
        /// <param name="sender">Usually the connection.</param>
        /// <param name="args">Arguments</param>
        public void DefaultUserJWTEventHandler(object sender, UserJWTEventArgs args)
        {
            args.JWT = UserJwt;
        }

        /// <summary>
        /// Utility method to signs the UserSignatureEventArgs server nonce from 
        /// a private credentials file.
        /// </summary>
        /// <param name="args">Arguments</param>
        public void SignNonce(UserSignatureEventArgs args)
        {
            // you have to load this every time b/c signing actually wipes data
            args.SignedNonce = JWTHandlerUtils.LoadNkeyPair(NkeySeed).Sign(args.ServerNonce);
        }

        /// <summary>
        /// The default User Signature event handler.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        public void DefaultUserSignatureHandler(object sender, UserSignatureEventArgs args)
        {
            SignNonce(args);
        }
    }
}
