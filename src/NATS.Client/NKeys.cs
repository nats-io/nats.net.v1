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
using NATS.Client.NaCl;
using System;

namespace NATS.Client
{
    internal static class Crc16
    {
        static ushort[] crc16tab = new ushort[] {
            0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
            0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
            0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
            0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
            0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
            0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
            0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
            0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
            0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
            0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
            0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
            0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
            0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
            0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
            0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
            0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
            0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
            0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
            0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
            0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
            0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
            0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
            0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
            0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
            0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
            0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
            0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
            0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
            0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
            0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
            0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
            0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0 };

        static internal ushort Checksum(byte[] data)
        {
            int crc = 0;
            foreach (byte b in data)
            {
                crc = ((crc << 8) & 0xffff) ^ crc16tab[((crc >> 8) ^ (ushort)b & 0x00FF)];
            }
            return (ushort)crc;
        }
    }

    /// <summary>
    /// Partial implementation of the NATS Ed25519 KeyPair.  This is not complete, but provides enough
    /// functionality to implement the client side NATS 2.0 security scheme.
    /// </summary>
    public class NkeyPair : IDisposable
    {
        private byte[] seed;
        private byte[] expandedPrivateKey;
        private byte[] key;

        internal NkeyPair(byte[] userSeed)
        {
            if (userSeed == null)
                throw new NATSException("seed cannot be null");

            int len = userSeed.Length;
            if (len != Ed25519.PrivateKeySeedSize)
                throw new NATSException("invalid seed length");

            seed = new byte[len];
            Buffer.BlockCopy(userSeed, 0, seed, 0, len);
            Ed25519.KeyPairFromSeed(out key, out expandedPrivateKey, seed);
        }

        /// <summary>
        /// Gets the public key of the keypair.
        /// </summary>
        public byte[] PublicKey { get { return key; } }

        /// <summary>
        /// Gets the private key of the keypair.
        /// </summary>
        public byte[] PrivateKeySeed { get { return seed; } }

        /// <summary>
        /// Wipes clean the internal private keys.
        /// </summary>
        public void Wipe()
        {
            Nkeys.Wipe(ref seed);
            Nkeys.Wipe(ref expandedPrivateKey);
        }

        /// <summary>
        /// Signs data and returns a signature.
        /// </summary>
        /// <param name="src"></param>
        /// <returns>The signature.</returns>
        public byte[] Sign(byte[] src)
        {
            byte[] rv =  Ed25519.Sign(src, expandedPrivateKey);
            CryptoBytes.Wipe(expandedPrivateKey);
            return rv;
        }

#region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Releases the unmanaged resources used by the NkeyPair and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Wipe();
                }
                key = null;
                disposedValue = true;
            }
        }

        /// <summary>
        /// Releases all resources used by the NkeyPair.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
#endregion
    }

    /// <summary>
    /// Nkeys is a class provided to manipulate Nkeys and generate NkeyPairs. 
    /// </summary>
    public class Nkeys
    {
        // PrefixByteSeed is the version byte used for encoded NATS Seeds
        const byte PrefixByteSeed = 18 << 3; // Base32-encodes to 'S...'

        // PrefixBytePrivate is the version byte used for encoded NATS Private keys
        const byte PrefixBytePrivate = 15 << 3; // Base32-encodes to 'P...'

        // PrefixByteServer is the version byte used for encoded NATS Servers
        const byte PrefixByteServer = 13 << 3; // Base32-encodes to 'N...'

        // PrefixByteCluster is the version byte used for encoded NATS Clusters
        const byte PrefixByteCluster = 2 << 3; // Base32-encodes to 'C...'

        // PrefixByteOperator is the version byte used for encoded NATS Operators
        const byte PrefixByteOperator = 14 << 3; // Base32-encodes to 'O...'

        // PrefixByteAccount is the version byte used for encoded NATS Accounts
        const byte PrefixByteAccount = 0; // Base32-encodes to 'A...'

        // PrefixByteUser is the version byte used for encoded NATS Users
        const byte PrefixByteUser = 20 << 3; // Base32-encodes to 'U...'

        // PrefixByteUnknown is for unknown prefixes.
        const byte PrefixByteUknown = 23 << 3; // Base32-encodes to 'X...'

        /// <summary>
        /// Decodes a base 32 encoded NKey into a nkey seed and verifies the checksum.
        /// </summary>
        /// <param name="src">Base 32 encoded Nkey.</param>
        /// <returns></returns>
        public static byte[] Decode(string src)
        {
            byte[] raw = Base32.Decode(src);
            ushort crc = (ushort)(raw[raw.Length - 2] | raw[raw.Length - 1] << 8);

            // trim off the CRC16
            int len = raw.Length - 2;
            byte[] data = new byte[len];
            Buffer.BlockCopy(raw, 0, data, 0, len);

            if (crc != Crc16.Checksum(data))
                throw new NATSException("Invalid CRC");
 
            return data;
        }

        private static bool IsValidPublicPrefixByte(byte prefixByte)
        {
            switch (prefixByte)
            {
                case PrefixByteServer:
                case PrefixByteCluster:
                case PrefixByteOperator:
                case PrefixByteAccount:
                case PrefixByteUser:
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Wipes a byte array.
        /// </summary>
        /// <param name="src">byte array to wipe</param>
        public static void Wipe(ref byte[] src)
        {
            CryptoBytes.Wipe(src);
        }

        /// <summary>
        /// Wipes a string.
        /// </summary>
        /// <param name="src">string to wipe</param>
        public static void Wipe(string src)
        {
            // best effort to wipe.
            if (src != null && src.Length > 0)
                src.Remove(0);
        }


        private static byte[] DecodeSeed(byte[] raw)
        {
            // Need to do the reverse here to get back to internal representation.
            byte b1 = (byte)(raw[0] & 248);  // 248 = 11111000
            byte b2 = (byte)((raw[0] & 7) << 5 | ((raw[1] & 248) >> 3)); // 7 = 00000111

            try
            {
                if (b1 != PrefixByteSeed)
                    throw new NATSException("Invalid Seed.");

                if (!IsValidPublicPrefixByte(b2))
                    throw new NATSException("Invalid Public Prefix Byte.");

                // Trim off the first two bytes
                byte[] data = new byte[raw.Length - 2];
                Buffer.BlockCopy(raw, 2, data, 0, data.Length);
                return data;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                Wipe(ref raw);
            }
        }

        private static byte[] DecodeSeed(string src)
        {
            return DecodeSeed(Nkeys.Decode(src));
        }

        /// <summary>
        /// Creates an NkeyPair from a private seed String.
        /// </summary>
        /// <param name="seed"></param>
        /// <returns>A NATS Ed25519 Keypair</returns>
        static public NkeyPair FromSeed(string seed)
        {
            byte[] userSeed = DecodeSeed(seed);
            try
            {
                var kp = new NkeyPair(userSeed);
                return kp;
            }
            finally
            {
                Wipe(ref userSeed);
            }
        }
    }
}
