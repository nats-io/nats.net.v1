using System;
using System.Security.Cryptography;

namespace NATS.Client.Internals
{
    internal sealed class Nuid
    {
        private const int PREFIX_LENGTH = 12;
        private const int SEQUENTIAL_LENGTH = 10;
        private const int NUID_LENGTH = PREFIX_LENGTH + SEQUENTIAL_LENGTH;
        private const int MIN_INCREMENT = 33;
        private const int MAX_INCREMENT = 333;
        private const long MAX_SEQUENTIAL = 1152921504606846976; // 64^10, 60 bits
        private const int BASE = 64;

        private static readonly byte[] _characters = new byte[BASE]{
            (byte)'0', (byte)'1', (byte)'2', (byte)'3', (byte)'4', (byte)'5', (byte)'6', (byte)'7',
            (byte)'8', (byte)'9', (byte)'A', (byte)'B', (byte)'C', (byte)'D', (byte)'E', (byte)'F',
            (byte)'G', (byte)'H', (byte)'I', (byte)'J', (byte)'K', (byte)'L', (byte)'M', (byte)'N',
            (byte)'O', (byte)'P', (byte)'Q', (byte)'R', (byte)'S', (byte)'T', (byte)'U', (byte)'V',
            (byte)'W', (byte)'X', (byte)'Y', (byte)'Z', (byte)'a', (byte)'b', (byte)'c', (byte)'d',
            (byte)'e', (byte)'f', (byte)'g', (byte)'h', (byte)'i', (byte)'j', (byte)'k', (byte)'l',
            (byte)'m', (byte)'n', (byte)'o', (byte)'p', (byte)'q', (byte)'r', (byte)'s', (byte)'t',
            (byte)'u', (byte)'v', (byte)'w', (byte)'x', (byte)'y', (byte)'z', (byte)'_', (byte)'-'
        };

        private readonly object _nuidLock = new object();

        private readonly Random _rng = new Random();
        private readonly object _rngLock = new object();
        private readonly RandomNumberGenerator _cryptoRng;
        private readonly object _cryptRngLock = new object();

        private char[] _prefix = new char[PREFIX_LENGTH];
        private int _increment;
        private long _sequential;

        /// <summary>
        /// Initializes a new instance of <see cref="Nuid"/>.
        /// </summary>
        /// <remarks>
        /// This constructor is intended to be used from unit tests and
        /// benchmarks only. For production use use <see cref="Nuid()"/> instead.
        /// </remarks>
        /// <param name="rng">A cryptographically strong random number generator.</param>
        /// <param name="sequential">The intitial sequential.</param>
        /// <param name="increment">The initial increment.</param>
        internal Nuid(RandomNumberGenerator rng = null, long? sequential = null, int? increment = null)
        {
            if (rng is null)
                _cryptoRng = RandomNumberGenerator.Create();
            else
                _cryptoRng = rng;
#if NET45
            // Instantiating System.Random multiple times in quick succession without a
            // proper seed may result in instances that yield identical sequences on .NET FX.
            // See https://docs.microsoft.com/en-us/dotnet/api/system.random?view=netframework-4.8#instantiating-the-random-number-generator
            // and https://docs.microsoft.com/en-us/dotnet/api/system.random?view=netframework-4.8#avoiding-multiple-instantiations
            var seedBytes = new byte[4];
            _cryptoRng.GetBytes(seedBytes);
            _rng = new Random(BitConverter.ToInt32(seedBytes, 0));
#endif
            if (sequential is null)
                _sequential = GetSequential();
            else
                _sequential = sequential.Value;

            if (increment is null)
                _increment = GetIncrement();
            else
                _increment = increment.Value;

            SetPrefix();
        }

        /// <summary>
        /// Initializes a new instance of <see cref="Nuid"/>.
        /// </summary>
        internal Nuid() : this(null) {}

        /// <summary>
        /// Returns a random NUID string.
        /// </summary>
        /// <returns>The NUID</returns>
        internal string GetNext()
        {
            var sequential = 0L;
            var nuidBuffer = new char[NUID_LENGTH];
            lock (_nuidLock)
            {
                sequential = _sequential += _increment;
                if(_sequential > MAX_SEQUENTIAL)
                {
                    SetPrefix();
                    sequential = _sequential = GetSequential();
                    _increment = GetIncrement();
                }
                Array.Copy(_prefix, nuidBuffer, _prefix.Length);
            }

            for(var i = PREFIX_LENGTH; i < NUID_LENGTH; i++)
            {
                nuidBuffer[i] = (char)_characters[sequential % BASE];
                sequential /= BASE;
            }

            return new string(nuidBuffer);
        }

        private int GetIncrement()
        {
            lock (_rngLock)
            {
                return _rng.Next(MIN_INCREMENT, MAX_INCREMENT);
            }
        }

        private long GetSequential()
        {
            var randomBytes = new byte[8];
            ulong seq;
            do
            {
                lock (_rngLock)
                {
                    _rng.NextBytes(randomBytes);
                }
                seq = BitConverter.ToUInt64(randomBytes, 0);

            } while (seq > ulong.MaxValue - ((ulong.MaxValue % MAX_SEQUENTIAL) + 1) % MAX_SEQUENTIAL);
            // Limit seq to MAX_SEQUENTIAL, see https://stackoverflow.com/a/13095144
            return (long)(seq % MAX_SEQUENTIAL);
        }

        //TODO: Synchronize access to _prefix
        private void SetPrefix()
        {
            var randomBytes = new byte[PREFIX_LENGTH];
            lock (_cryptRngLock)
            {
                _cryptoRng.GetBytes(randomBytes);
            }

            for(var i = 0; i < randomBytes.Length; i++)
            {
                _prefix[i] = (char)_characters[randomBytes[i] % BASE];
            }

        }
    }
}
