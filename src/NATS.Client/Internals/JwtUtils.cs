// Copyright 2022-2023 The NATS Authors
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
using System.Security.Cryptography;
using System.Text;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using static NATS.Client.EncodingUtils;
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.Internals
{
    public static class JwtUtils
    {

        public static readonly string EncodedClaimHeader =
            ToBase64UrlEncoded(Encoding.ASCII.GetBytes("{\"typ\":\"JWT\", \"alg\":\"ed25519-nkey\"}"));

        public static readonly long NoLimit = -1;

        /// <summary>
        /// Format string with `%s` placeholder for the JWT token followed
        /// by the user NKey seed. This can be directly used as such:
        /// <pre>
        /// NKey userKey = NKey.createUser(new SecureRandom());
        /// NKey signingKey = loadFromSecretStore();
        /// string jwt = IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey);
        /// string.format(JwtUtils.NatsUserJwtFormat, jwt, userKey.EncodedSeed);
        /// </pre>
        /// </summary>
        public static readonly string NatsUserJwtFormat =
            "-----BEGIN NATS USER JWT-----\n" +
            "{0}\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "    NKEY Seed printed below can be used to sign and prove identity.\n" +
            "    NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "{1}\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";

        public static long UnixTimeSeconds()
        {
            return DateTimeOffset.Now.ToUnixTimeSeconds();
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="accountId">a mandatory public account nkey. Will throw error when not set or not account nkey.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string accountId, string publicUserKey)
        {
            return IssueUserJWT(signingKey, publicUserKey, null, null, UnixTimeSeconds(), null,
                new UserClaim(accountId));
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="accountId">a mandatory public account nkey. Will throw error when not set or not account nkey.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <param name="name">optional human-readable name. When absent, default to publicUserKey.</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string accountId, string publicUserKey, string name)
        {
            return IssueUserJWT(signingKey, publicUserKey, name, null, UnixTimeSeconds(), null, new UserClaim(accountId));
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="accountId">a mandatory public account nkey. Will throw error when not set or not account nkey.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <param name="name">optional human-readable name. When absent, default to publicUserKey.</param>
        /// <param name="expiration">optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.</param>
        /// <param name="tags">optional list of tags to be included in the JWT.</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string accountId, string publicUserKey, string name, Duration expiration, params string[] tags)
        {
            return IssueUserJWT(signingKey, publicUserKey, name, expiration, UnixTimeSeconds(), null, new UserClaim(accountId, tags));
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="accountId">a mandatory public account nkey. Will throw error when not set or not account nkey.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <param name="name">optional human-readable name. When absent, default to publicUserKey.</param>
        /// <param name="expiration">optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.</param>
        /// <param name="tags">optional list of tags to be included in the JWT.</param>
        /// <param name="issuedAt">the current epoch seconds.</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string accountId, string publicUserKey, string name, Duration expiration, string[] tags, long issuedAt)
        {
            return IssueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, null, new UserClaim(accountId, tags));
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="accountId">a mandatory public account nkey. Will throw error when not set or not account nkey.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <param name="name">optional human-readable name. When absent, default to publicUserKey.</param>
        /// <param name="expiration">optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.</param>
        /// <param name="tags">optional list of tags to be included in the JWT.</param>
        /// <param name="issuedAt">the current epoch seconds.</param>
        /// <param name="audience">optional audience</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string accountId, string publicUserKey, string name, Duration expiration, string[] tags, long issuedAt, string audience)
        {
            return IssueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, audience, new UserClaim(accountId, tags));
        }

        /// <summary>
        /// Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
        /// </summary>
        /// <param name="signingKey">a mandatory account nkey pair to sign the generated jwt.</param>
        /// <param name="publicUserKey">a mandatory public user nkey. Will throw error when not set or not user nkey.</param>
        /// <param name="name">optional human-readable name. When absent, default to publicUserKey.</param>
        /// <param name="expiration">optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.</param>
        /// <param name="issuedAt">the current epoch seconds.</param>
        /// <param name="audience">optional audience</param>
        /// <param name="nats">the user claim</param>
        /// <returns>a JWT</returns>
        public static string IssueUserJWT(NkeyPair signingKey, string publicUserKey, string name, Duration expiration, long issuedAt, string audience, UserClaim nats)
        {
            // Validate the signingKey:
            if (signingKey.Type != Nkeys.PrefixType.Account)
            {
                throw new ArgumentException(
                    "IssueUserJWT requires an account key for the signingKey parameter, but got " + signingKey.Type);
            }

            // Validate the accountId:
            NkeyPair accountKey = Nkeys.FromPublicKey(nats.IssuerAccount.ToCharArray());
            if (accountKey.Type != Nkeys.PrefixType.Account)
            {
                throw new ArgumentException(
                    "IssueUserJWT requires an account key for the accountId parameter, but got " + accountKey.Type);
            }

            // Validate the publicUserKey:
            NkeyPair userKey = Nkeys.FromPublicKey(publicUserKey.ToCharArray());
            if (userKey.Type != Nkeys.PrefixType.User)
            {
                throw new ArgumentException("IssueUserJWT requires a user key for the publicUserKey parameter, but got " + userKey.Type);
            }

            string accSigningKeyPub = signingKey.EncodedPublicKey;

            string claimName = string.IsNullOrWhiteSpace(name) ? publicUserKey : name;

            return issueJWT(signingKey, publicUserKey, claimName, expiration, issuedAt, accSigningKeyPub, audience,
                nats);
        }

        /// <summary>
        /// Issue a JWT
        /// </summary>
        /// <param name="signingKey">account nkey pair to sign the generated jwt.</param>
        /// <param name="publicUserKey">a mandatory public user nkey.</param>
        /// <param name="name">optional human-readable name.</param>
        /// <param name="expiration">optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.</param>
        /// <param name="issuedAt">the current epoch seconds.</param>
        /// <param name="accSigningKeyPub">the account signing key</param>
        /// <param name="audience">optional audience</param>
        /// <param name="nats">the generic nats claim</param>
        /// <returns>a JWT</returns>
        public static string issueJWT(NkeyPair signingKey, string publicUserKey, string name, Duration expiration,
            long issuedAt, string accSigningKeyPub, string audience, JsonSerializable nats)
        {
            Claim claim = new Claim();
            claim.Aud = audience;
            claim.Iat = issuedAt;
            claim.Iss = accSigningKeyPub;
            claim.Name = name;
            claim.Sub = publicUserKey;
            claim.Exp = expiration;
            claim.Nats = nats;

            // Issue At time is stored in unix seconds
            string claimJson = claim.ToJsonString();

            // Compute jti, a base32 encoded sha256 hash
            IncrementalHash hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            hasher.AppendData(Encoding.ASCII.GetBytes(claimJson));

            claim.Jti = Base32.Encode(hasher.GetHashAndReset());

            // all three components (header/body/signature) are base64url encoded
            string encBody = ToBase64UrlEncoded(claim.Serialize());

            // compute the signature off of header + body (. included on purpose)
            byte[] sig = Encoding.ASCII.GetBytes(EncodedClaimHeader + "." + encBody);
            string encSig = ToBase64UrlEncoded(signingKey.Sign(sig));

            // append signature to header and body and return it
            return EncodedClaimHeader + "." + encBody + "." + encSig;
        }

        /// <summary>
        /// Get the claim body from a JWT
        /// </summary>
        /// <param name="jwt">the encoded jwt</param>
        /// <returns>the claim body json</returns>
        public static string GetClaimBody(string jwt)
        {
            return FromBase64UrlEncoded(jwt.Split('.')[1]);
        }
    }

    public class UserClaim : JsonSerializable {
        public string IssuerAccount;            // User
        public string[] Tags;                   // User/GenericFields
        public string Type = "user";            // User/GenericFields
        public int Version = 2;                 // User/GenericFields
        public Permission Pub;                  // User/UserPermissionLimits/Permissions
        public Permission Sub;                  // User/UserPermissionLimits/Permissions
        public ResponsePermission Resp;         // User/UserPermissionLimits/Permissions
        public string[] Src;                    // User/UserPermissionLimits/Limits/UserLimits
        public IList<TimeRange> Times;          // User/UserPermissionLimits/Limits/UserLimits
        public string Locale;                   // User/UserPermissionLimits/Limits/UserLimits
        public long Subs = JwtUtils.NoLimit;    // User/UserPermissionLimits/Limits/NatsLimits
        public long Data = JwtUtils.NoLimit;    // User/UserPermissionLimits/Limits/NatsLimits
        public long Payload = JwtUtils.NoLimit; // User/UserPermissionLimits/Limits/NatsLimits
        public bool BearerToken;                // User/UserPermissionLimits
        public string[] AllowedConnectionTypes; // User/UserPermissionLimits
    
        public UserClaim(string issuerAccount) {
            this.IssuerAccount = issuerAccount;
        }
    
        public UserClaim(string issuerAccount, string[] tags) {
            IssuerAccount = issuerAccount;
            Tags = tags;
        }
    
        public override JSONNode ToJsonNode() {
            JSONObject o = new JSONObject();
            AddField(o, "issuer_account", IssuerAccount);
            AddField(o, "tags", Tags);
            AddField(o, "type", Type);
            AddField(o, "version", Version);
            AddField(o, "pub", Pub);
            AddField(o, "sub", Sub);
            AddField(o, "resp", Resp);
            AddField(o, "src", Src);
            AddField(o, "times", Times);
            AddField(o, "times_location", Locale);
            AddFieldWhenGteMinusOne(o, "subs", Subs);
            AddFieldWhenGteMinusOne(o, "data", Data);
            AddFieldWhenGteMinusOne(o, "payload", Payload);
            AddField(o, "bearer_token", BearerToken);
            AddField(o, "allowed_connection_types", AllowedConnectionTypes);
            return o;
        }
    }
    
    public class TimeRange : JsonSerializable {
        public string Start;
        public string End;
    
        public TimeRange(string start, string end) {
            Start = start;
            End = end;
        }
    
        public override JSONNode ToJsonNode() {
            JSONObject o = new JSONObject();
            AddField(o, "start", Start);
            AddField(o, "end", End);
            return o;
        }
    }
    
    public class ResponsePermission : JsonSerializable {
        public int MaxMsgs;
        public Duration Expires;
    
        public override JSONNode ToJsonNode() {
            JSONObject o = new JSONObject();
            AddField(o, "max", MaxMsgs);
            AsDuration(o, "ttl", Expires);
            return o;
        }
    }
    
    public class Permission : JsonSerializable {
        public string[] Allow;
        public string[] Deny;
    
        public override JSONNode ToJsonNode() {
            JSONObject o = new JSONObject();
            AddField(o, "allow", Allow);
            AddField(o, "deny", Deny);
            return o;
        }
    }
    
    public class Claim : JsonSerializable {
        public string Aud;
        public string Jti;
        public long Iat;
        public string Iss;
        public string Name;
        public string Sub;
        public Duration Exp;
        public JsonSerializable Nats;
    
        public override JSONNode ToJsonNode() {
            JSONObject o = new JSONObject();

            AddField(o, "aud", Aud);
            AddFieldEvenEmpty(o, "jti", Jti);
            AddField(o, "iat", Iat);
            AddField(o, "iss", Iss);
            AddField(o, "name", Name);
            AddField(o, "sub", Sub);
            
            if (Exp != null && !Exp.IsZero() && !Exp.IsNegative()) {
                long seconds = Exp.Millis / 1000;
                AddField(o, "exp", Iat + seconds);  // relative to the iat
            }
            
            AddField(o, "nats", Nats);
            return o;
        }
    }
}
