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
using System.Security.Cryptography;

namespace NATS.Client
{
    /// <summary>
    /// NUID needs to be very fast to generate and truly unique, all while being entropy pool friendly.
    /// We will use 12 bytes of crypto generated data (entropy draining), and 10 bytes of sequential data
    /// that is started at a pseudo random number and increments with a pseudo-random increment.
    /// Total is 22 bytes of base 36 ascii text.
    /// </summary>
    [Obsolete("NATS.Client.NUID is deprecated and will be removed in a future version")]
    public class NUID
    {
        private static char[] digits = 
            { '0','1','2','3','4','5','6','7','8','9',
              'A','B','C','D','E','F','G','H','I','J',
              'K','L','M','N','O','P','Q','R','S','T',
              'U','V','W','X','Y','Z' };

        private static int  nuidBase = 36;
        private static int  preLen = 12;
        private static long seqLen = 10;

        /// <summary>
        /// Length of the NUID.
        /// </summary>
        public static  long LENGTH = preLen + seqLen;

        /// <summary>
        /// Maximum value of the prefix.
        /// </summary>
        public static  long MAXPRE = 4738381338321616896L;	// base^preLen == 36^12

        /// <summary>
        /// Maximum value of the sequence.
        /// </summary>
        public static  long MAXSEQ = 3656158440062976L; 	// base^seqLen == 36^10

        private static int  minInc = 33;
        private static int  maxInc = 333;
        private static long totalLen = preLen + seqLen;
#if NET46
        private RNGCryptoServiceProvider srand = new RNGCryptoServiceProvider();
#else
        private RandomNumberGenerator srand = RandomNumberGenerator.Create();
#endif
        private Random prand = new Random();

        // Instance fields
        private byte[] pre;
        private long seq;
        private long inc;

        // Global NUID
        private static NUID globalNUID = new NUID();
        private static Object globalLock = new Object();

        private long nextLong(bool useSecureRand, long min, long max)
        {
            ulong uRange = (ulong)(max - min);

            ulong ulongRand;
            do
            {
                byte[] buf = new byte[8];

                if (useSecureRand)
                {
                    srand.GetBytes(buf);
                }
                else
                {
                    prand.NextBytes(buf);
                }
            
                ulongRand = (ulong)BitConverter.ToInt64(buf, 0);
            
            } while (ulongRand > ulong.MaxValue - ((ulong.MaxValue % uRange) + 1) % uRange);

            return (long)(ulongRand % uRange) + min;
        }

        private long nextLong(bool useSecureRand, long max)
        {
            return nextLong(useSecureRand, 0, max);
        }

        private long nextLong(bool useSecureRand)
        {
            return nextLong(useSecureRand, long.MinValue, long.MaxValue);
        }

        private string getNextNuid()
        {
            seq += inc;
            if (seq >= MAXSEQ)
            {
                RandomizePrefix();
                resetSequential();
            }

            char[] b = new char[totalLen];
            Array.Copy(pre, b, preLen);

            int i = b.Length;
            for (long l = seq; i > preLen; l /= nuidBase)
            {
                i--;
                b[i] = digits[(int)(l % nuidBase)];
            }

            return new String(b);
        }

        private void resetSequential()
        {
            seq = nextLong(false, MAXSEQ);
            inc = nextLong(false, minInc, maxInc);
        }

        /// <summary>
        /// Generates a new crypto/rand seeded prefix.
        /// </summary>
        /// <remarks>
        /// Generally not needed, this happens automatically.
        /// </remarks>
        public void RandomizePrefix()
        {
            long n = nextLong(true, MAXPRE);
            int i = pre.Length;
            for (long l = n; l > 0; l /= nuidBase)
            {
                i--;
                pre[i] = (byte)digits[(int)(l % nuidBase)];
            }
        }

        static NUID()
        {
            globalNUID = new NUID();
            globalNUID.RandomizePrefix();
        }

        /// <summary>
        /// Creates a new NUID object.
        /// </summary>
        public NUID()
        {
            seq = nextLong(false, MAXSEQ);
            inc = nextLong(false, minInc, maxInc);
            pre = new byte[preLen];
            RandomizePrefix();
        }

        /// <summary>
        /// Gets the global instance of a NUID object
        /// </summary>
        static public NUID Instance
        {
            get
            {
                return globalNUID;
            }
        }

        /// <summary>
        /// Returns the next NUID from the global instance.
        /// </summary>
        static public string NextGlobal
        {
            get
            {
                lock (globalLock)
                {
                    return globalNUID.getNextNuid();
                }
            }
        }
        /// <summary>
        /// Returns the next nuid string value from the NUID object.
        /// </summary>
        public string Next
        {
            get
            {
                return getNextNuid();
            }
        }

        /// <summary>
        /// Gets or sets the prefix.
        /// </summary>
        /// <remarks>
        /// Not normally used outside of testing.
        /// </remarks>
        public byte[] Pre
        {
            get
            {
                return pre;
            }
            set
            {
                pre = value;
            }
        }

        /// <summary>
        /// Gets or sets the sequence.  Not normally used outside of testing.
        /// </summary>
        /// <remarks>
        /// Not normally used outside of testing.
        /// </remarks>
        public long Seq
        {
            get
            {
                return seq;
            }
            set
            {
                seq = value;
            }
        }

        /// <summary>
        /// Gets the Length of the nuid.
        /// </summary>
        /// <remarks>
        /// Not normally used outside of testing.
        /// </remarks>
        public long Length
        {
            get
            {
                return seqLen + preLen;
            }
        }
    }
}
