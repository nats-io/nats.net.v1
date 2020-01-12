using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using NATS.Client.Internals;
using Xunit;

namespace UnitTests.Internals
{
    public class TestNuid
    {
        [Fact]
        public void GetNextNuid_ReturnsNuidOfLength22()
        {
            // Arrange
            var nuid = new Nuid();

            //Act
            var result = nuid.GetNext();

            // Assert
            Assert.Equal(22, result.Length);
        }

        [Fact]
        public void GetNextNuid_ReturnsDifferentNuidEachTime()
        {
            // Arrange
            var nuid = new Nuid();

            // Act
            var firstNuid = nuid.GetNext();
            var secondNuid = nuid.GetNext();

            // Assert
            Assert.NotEqual(firstNuid, secondNuid);
        }

        [Fact]
        public void GetNextNuid_PrefixIsConstant()
        {
            // Arrange
            var nuid = new Nuid();

            // Act
            var firstNuid = nuid.GetNext().Substring(0, 12);
            var secondNuid = nuid.GetNext().Substring(0, 12);

            // Assert
            Assert.Equal(firstNuid, secondNuid);
        }

        [Fact]
        public void GetNextNuid_ContainsOnlyValidCharacters()
        {
            // Arrange
            var nuid = new Nuid();

            // Act
            var result = nuid.GetNext();

            // Assert
            Assert.Matches("[A-z0-9_-]{22}", result);
        }

        [Fact]
        public void GetNextNuid_PrefixRenewed()
        {
            // Arrange
            var increment = 100;
            var maxSequential = 1152921504606846976 - increment;
            var nuid = new Nuid(RandomNumberGenerator.Create(), maxSequential, increment);

            // Act
            var firstNuid = nuid.GetNext().Substring(0, 12);
            var secondNuid = nuid.GetNext().Substring(0, 12);

            // Assert
            Assert.NotEqual(firstNuid, secondNuid);
        }

        [Fact]
        public void GetNextNuid_PrefixAsExpected()
        {
            // Arrange
            var rngBytes = new byte[12] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            var rng = new ControlledRng(new Queue<byte[]>(new byte[][] { rngBytes, rngBytes }));

            var nuid = new Nuid(rng);

            // Act
            var prefix = nuid.GetNext().Substring(0, 12);

            // Assert
            Assert.Equal("0123456789AB", prefix);
        }

        [Fact]
        public void NuidInitialization_RngInvokedOnce()
        {
            // Arrange
            var rngBytes = new byte[12] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            var rng = new ControlledRng(new Queue<byte[]>(new byte[][] { rngBytes, rngBytes }));

            // Act
            var nuid = new Nuid(rng); ;

            // Assert
#if NET452
            Assert.Equal(2, rng.GetBytesInvocations);
#else
            Assert.Equal(1, rng.GetBytesInvocations);
#endif
        }

        private class ControlledRng : RandomNumberGenerator
        {
            public int GetBytesInvocations = 0;
            private Queue<byte[]> _bytes;

            public ControlledRng(Queue<byte[]> bytes)
            {
                _bytes = bytes;
            }

            public override void GetBytes(byte[] data)
            {
                var nextBytes = _bytes.Dequeue();
                if (nextBytes.Length < data.Length)
                    throw new InvalidOperationException($"Lenght of {nameof(data)} is {data.Length}, length of {nameof(nextBytes)} is {nextBytes.Length}");

                Array.Copy(nextBytes, data, data.Length);
                GetBytesInvocations++;
            }
        }
    }
}
