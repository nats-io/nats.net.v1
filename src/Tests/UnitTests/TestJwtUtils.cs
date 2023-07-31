// Copyright 2015-2018 The NATS Authors
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
using NATS.Client;
using NATS.Client.Internals;
using Xunit;

namespace UnitTests
{
    public class TestJwtUtils
    {
        [Fact]
        public void IssueUserJWTSuccessMinimal()
        {
            NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
            NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
            string accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
            string jwt = JwtUtils.IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey, null, null, null, 1633043378, "audience");
            string claimBody = JwtUtils.GetClaimBody(jwt);
            string cred = string.Format(JwtUtils.NatsUserJwtFormat, jwt, userKey.EncodedSeed);
            /*
                Generated JWT:
                {
	                "aud": "audience",
	                "jti": "QAT6N5ETHLTXUWE2FF6RLNNPLMGEYQVEOMFNWDDMKK5RSPG4A7BQ",
	                "iat": 1633043378,
	                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
	                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
	                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
	                "nats": {
		                "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
		                "type": "user",
		                "version": 2,
		                "subs": -1,
		                "data": -1,
		                "payload": -1
	                }
                }
            */
            string expectedClaimBody = "{\"aud\":\"audience\",\"jti\":\"QAT6N5ETHLTXUWE2FF6RLNNPLMGEYQVEOMFNWDDMKK5RSPG4A7BQ\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}}";
            string expectedCred = 
                "-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJhdWQiOiJhdWRpZW5jZSIsImp0aSI6IlFBVDZONUVUSExUWFVXRTJGRjZSTE5OUExNR0VZUVZFT01GTldERE1LSzVSU1BHNEE3QlEiLCJpYXQiOjE2MzMwNDMzNzgsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIiwic3ViIjoiVUE2S09NUTY3WE9FM0ZIRTM3VzRPWEFEVlhWWUlTQk5MVEJVVDJMU1k1VkZLQUlKN0NSRFIyUloiLCJuYXRzIjp7Imlzc3Vlcl9hY2NvdW50IjoiQUNYWlJBTElMMjJXUkVURFJYWUtPWURCN1hDM0U3TUJTVlVTVU1GQUNPNk9NNVZQUk5GTU9PTzYiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjIsInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTF9fQ.PrQSCBknSmOB6GRh_2YzW6mQlXYhjALACfsU0lzKhwVmqGSefIySx9saTb9asZle1uFSk9Bfkn0NCWPstDx8Bw\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "    NKEY Seed printed below can be used to sign and prove identity.\n" +
                "    NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n";
            Assert.Equal(expectedClaimBody, claimBody);
            Assert.Equal(expectedCred, cred);
        }
        
        [Fact]
        public void IssueUserJWTSuccessAllArgs()
        {
            NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
            NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
            string accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
            string jwt = JwtUtils.IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey, "name", Duration.OfSeconds(100), new string[]{"tag1", "tag\\two"}, 1633043378, "audience");
            string claimBody = JwtUtils.GetClaimBody(jwt);
            string cred = string.Format(JwtUtils.NatsUserJwtFormat, jwt, userKey.EncodedSeed);
            /*
                Generated JWT:
				{
					"aud": "audience",
					"jti": "KJ3QLQK4HG3Y6VJO74JS7YGGETARBXBOMMIHG3RDC4L2YXGXKCPQ",
					"iat": 1633043378,
					"iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
					"name": "name",
					"sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
					"exp": 1633043478,
					"nats": {
						"issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
						"tags": ["tag1", "tag\\two"],
						"type": "user",
						"version": 2,
						"subs": -1,
						"data": -1,
						"payload": -1
					}
				}
            */
            string expectedClaimBody = "{\"aud\":\"audience\",\"jti\":\"KJ3QLQK4HG3Y6VJO74JS7YGGETARBXBOMMIHG3RDC4L2YXGXKCPQ\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"name\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"exp\":1633043478,\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"tags\":[\"tag1\",\"tag\\\\two\"],\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}}";
            string expectedCred = 
                "-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJhdWQiOiJhdWRpZW5jZSIsImp0aSI6IktKM1FMUUs0SEczWTZWSk83NEpTN1lHR0VUQVJCWEJPTU1JSEczUkRDNEwyWVhHWEtDUFEiLCJpYXQiOjE2MzMwNDMzNzgsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6Im5hbWUiLCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsImV4cCI6MTYzMzA0MzQ3OCwibmF0cyI6eyJpc3N1ZXJfYWNjb3VudCI6IkFDWFpSQUxJTDIyV1JFVERSWFlLT1lEQjdYQzNFN01CU1ZVU1VNRkFDTzZPTTVWUFJORk1PT082IiwidGFncyI6WyJ0YWcxIiwidGFnXFx0d28iXSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyLCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xfX0.bnDq39ioNCYUFrRshxmUKWcBU5qXTTSiTmC7QIbC6kJCKbOB7TrAaqJpmS_vwXAotS1JcYsy4URtTmoyiYlgDw\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "    NKEY Seed printed below can be used to sign and prove identity.\n" +
                "    NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n";
            Assert.Equal(expectedClaimBody, claimBody);
            Assert.Equal(expectedCred, cred);
        }
                
        [Fact]
        public void IssueUserJWTSuccessCustom()
        {
            NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
            NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
            UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6");
            userClaim.Pub = new Permission();
            userClaim.Pub.Allow = new string[] { "pub-allow-subject" };
            userClaim.Pub.Deny = new string[] { "pub-deny-subject" };
            userClaim.Sub = new Permission();
            userClaim.Sub.Allow = new string[] { "sub-allow-subject" };
            userClaim.Sub.Deny = new string[] { "sub-deny-subject" };
            userClaim.Tags = new string[]{"tag1", "tag\\two"};

            string jwt = JwtUtils.IssueUserJWT(signingKey, userKey.EncodedPublicKey, "custom", null, 1633043378, null, userClaim);
            string claimBody = JwtUtils.GetClaimBody(jwt);
            string cred = string.Format(JwtUtils.NatsUserJwtFormat, jwt, userKey.EncodedSeed);
            /*
                Generated JWT:
				{
					"jti": "HLGJ6JO3YYHWQDUOPTLZCZQ5EUPS57PABGICO75ZNIMNEXZNIWIQ",
					"iat": 1633043378,
					"iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
					"name": "custom",
					"sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
					"nats": {
						"issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
						"tags": ["tag1", "tag\\two"],
						"type": "user",
						"version": 2,
						"pub": {
							"allow": ["pub-allow-subject"],
							"deny": ["pub-deny-subject"]
						},
						"sub": {
							"allow": ["sub-allow-subject"],
							"deny": ["sub-deny-subject"]
						},
						"subs": -1,
						"data": -1,
						"payload": -1
					}
				}
            */
            string expectedClaimBody = "{\"jti\":\"HLGJ6JO3YYHWQDUOPTLZCZQ5EUPS57PABGICO75ZNIMNEXZNIWIQ\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"custom\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"tags\":[\"tag1\",\"tag\\\\two\"],\"type\":\"user\",\"version\":2,\"pub\":{\"allow\":[\"pub-allow-subject\"],\"deny\":[\"pub-deny-subject\"]},\"sub\":{\"allow\":[\"sub-allow-subject\"],\"deny\":[\"sub-deny-subject\"]},\"subs\":-1,\"data\":-1,\"payload\":-1}}";
            string expectedCred = 
                "-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJqdGkiOiJITEdKNkpPM1lZSFdRRFVPUFRMWkNaUTVFVVBTNTdQQUJHSUNPNzVaTklNTkVYWk5JV0lRIiwiaWF0IjoxNjMzMDQzMzc4LCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJjdXN0b20iLCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInRhZ3MiOlsidGFnMSIsInRhZ1xcdHdvIl0sInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6MiwicHViIjp7ImFsbG93IjpbInB1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsicHViLWRlbnktc3ViamVjdCJdfSwic3ViIjp7ImFsbG93IjpbInN1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsic3ViLWRlbnktc3ViamVjdCJdfSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMX19.LSvZ41HZs7Rt43qGGI94q34Fe8SsUIthvFJ1Cr603UmvCiI1gNd1fEBwghQUxw8sVP9y56ZC6tQ6Pld-RhTpBg\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "    NKEY Seed printed below can be used to sign and prove identity.\n" +
                "    NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n";
            Assert.Equal(expectedClaimBody, claimBody);
            Assert.Equal(expectedCred, cred);
        }
                        
        [Fact]
        public void IssueUserJWTSuccessCustomLimits()
        {
            NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
            NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
            UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6");
            userClaim.Subs = 1;
            userClaim.Data = 2;
            userClaim.Payload = 3;

            string jwt = JwtUtils.IssueUserJWT(signingKey, userKey.EncodedPublicKey, "custom", null, 1633043378, null, userClaim);
            string claimBody = JwtUtils.GetClaimBody(jwt);
            string cred = string.Format(JwtUtils.NatsUserJwtFormat, jwt, userKey.EncodedSeed);
            /*
                Generated JWT:
				{
					"jti": "Q6HIBZQC4VDOE6UKPPNMT5K5XV4TZDO4UXLJ56IVB75EAXRIDFTA",
					"iat": 1633043378,
					"iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
					"name": "custom",
					"sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
					"nats": {
						"issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
						"type": "user",
						"version": 2,
						"subs": 1,
						"data": 2,
						"payload": 3
					}
				}
            */
            string expectedClaimBody = "{\"jti\":\"Q6HIBZQC4VDOE6UKPPNMT5K5XV4TZDO4UXLJ56IVB75EAXRIDFTA\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"custom\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"type\":\"user\",\"version\":2,\"subs\":1,\"data\":2,\"payload\":3}}";
            string expectedCred = 
                "-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJqdGkiOiJRNkhJQlpRQzRWRE9FNlVLUFBOTVQ1SzVYVjRUWkRPNFVYTEo1NklWQjc1RUFYUklERlRBIiwiaWF0IjoxNjMzMDQzMzc4LCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJjdXN0b20iLCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Miwic3VicyI6MSwiZGF0YSI6MiwicGF5bG9hZCI6M319.4ZPq0ZWlenG2HzobgICZ_Ichp6axnw4M-dmGKqn87EPjkFF7Ay_7GFKW38oqjZMTe7wuZa0ULMbLMjeEwmugCg\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "    NKEY Seed printed below can be used to sign and prove identity.\n" +
                "    NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n";
            Assert.Equal(expectedClaimBody, claimBody);
            Assert.Equal(expectedCred, cred);
        }

        [Fact]
        public void IssueUserJWTBadSigningKey()
        {
	        NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
	        // should be account, but this is a user key:
	        NkeyPair signingKey = Nkeys.FromSeed("SUAIW7IZ2YDQYLTE4FJ64ZBX7UMLCN57V6GHALKMUSMJCU5PJDNUO6BVUI");
	        string accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";

	        ArgumentException e = Assert.Throws<ArgumentException>(() =>
		        JwtUtils.IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey, null, null, null, 1633043378));
	        Assert.Equal("IssueUserJWT requires an account key for the signingKey parameter, but got User", e.Message);
        }

        [Fact]
        public void IssueUserJWTBadAccountId()
        {
	        NkeyPair userKey = Nkeys.FromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY");
	        NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
	        // should be account, but this is a user key:
	        string accountId = "UDN6WZFPYTS4YSUHUD4YFFU5NVKT6BVCY5QXQFYF3I23AER622SBOVUZ";

	        ArgumentException e = Assert.Throws<ArgumentException>(() =>
		        JwtUtils.IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey, null, null, null, 1633043378));
	        Assert.Equal("IssueUserJWT requires an account key for the accountId parameter, but got User", e.Message);
        }

        [Fact]
        public void IssueUserJWTBadPublicUserKey()
        {
	        NkeyPair userKey = Nkeys.FromSeed("SAADFHQTEKYBOCG4CPEPNAJ5FLRX4G4WTCNTAIOKN3LARLHGVKB4BRUHYY");
	        NkeyPair signingKey = Nkeys.FromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU");
	        string accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";

	        ArgumentException e = Assert.Throws<ArgumentException>(() =>
		        JwtUtils.IssueUserJWT(signingKey, accountId, userKey.EncodedPublicKey, null, null, null, 1633043378));
	        Assert.Equal("IssueUserJWT requires a user key for the publicUserKey parameter, but got Account", e.Message);
        }

        [Fact]
        public void TestUserClaimJson()
        {
	        UserClaim userClaim = new UserClaim("test-issuer-account");
	        Assert.Equal(BasicJson, userClaim.ToJsonString());

	        ResponsePermission resp = new ResponsePermission();
	        resp.MaxMsgs = 99;
	        resp.Expires = Duration.OfMillis(999);
	        
	        IList<TimeRange> times = new List<TimeRange>();
	        times.Add(new TimeRange("01:15:00", "03:15:00"));

	        userClaim.Resp = resp;
	        userClaim.Src = new []{"src1", "src2"};
	        userClaim.Times = times;
	        userClaim.Locale = "US/Eastern";
	        userClaim.Subs = 42;
	        userClaim.Data = 43;
	        userClaim.Payload = 44;
	        userClaim.BearerToken = true;
	        userClaim.AllowedConnectionTypes = new []{"nats", "tls"};

	        Assert.Equal(FullJson, userClaim.ToJsonString());
        }
 
        private const string BasicJson = "{\"issuer_account\":\"test-issuer-account\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}";
        private const string FullJson = "{\"issuer_account\":\"test-issuer-account\",\"type\":\"user\",\"version\":2,\"resp\":{\"max\":99},\"src\":[\"src1\",\"src2\"],\"times\":[{\"start\":\"01:15:00\",\"end\":\"03:15:00\"}],\"times_location\":\"US/Eastern\",\"subs\":42,\"data\":43,\"payload\":44,\"bearer_token\":true,\"allowed_connection_types\":[\"nats\",\"tls\"]}";
        
        /*
			Basic Json
			{
				"issuer_account": "test-issuer-account",
				"type": "user",
				"version": 2,
				"subs": -1,
				"data": -1,
				"payload": -1
			}		
				
			Full Json
			{
				"issuer_account": "test-issuer-account",
				"type": "user",
				"version": 2,
				"resp": {
					"max": 99
				},
				"src": ["src1", "src2"],
				"times": [{
					"start": "01:15:00",
					"end": "03:15:00"
				}],
				"times_location": "US/Eastern",
				"subs": 42,
				"data": 43,
				"payload": 44,
				"bearer_token": true,
				"allowed_connection_types": ["nats", "tls"]
			}			
         */
    }
}