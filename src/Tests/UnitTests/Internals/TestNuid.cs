using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using NATS.Client.Internals;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests.Internals
{
    public class TestNuid
    {
        private readonly ITestOutputHelper _outputHelper;

        public TestNuid(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
        }
        
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
            Assert.Matches("[A-z0-9+/]{22}", result);
        }

        [Fact]
        public void GetNextNuid_PrefixRenewed()
        {
            // Arrange
            var increment = 100;
            var maxSequential = 0x1000_0000_0000_0000 - increment - 1;
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
            var rngBytes = new byte[12] { 0, 1, 2, 3, 4, 5, 6, 7, 11, 253, 254, 255 };
            var rng = new ControlledRng(new Queue<byte[]>(new byte[][] { rngBytes, rngBytes }));

            var nuid = new Nuid(rng);

            // Act
            var prefix = nuid.GetNext().Substring(0, 12);

            // Assert
            Assert.Equal("ABCDEFGHL9+/", prefix);
        }

        [Fact]
        public void NuidInitialization_RngInvokedOnce()
        {
            // Arrange
            var rngBytes = new byte[12] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            var rng = new ControlledRng(new Queue<byte[]>(new[] { rngBytes, rngBytes }));

            // Act
            var nuid = new Nuid(rng); ;

            // Assert
#if NET452
            // On .NET FX we have an additional invocation for seeding System.Random
            Assert.Equal(2, rng.GetBytesInvocations);
#else
            Assert.Equal(1, rng.GetBytesInvocations);
#endif
        }
        
        [Fact]
        public void GetNextNuid_NuidsAreUnique()
        {
            // Arrange
            const int count = 1_000_000;
            var nuid = new Nuid();
            var nuids = new HashSet<string>(StringComparer.Ordinal);

            // Act
            for (var i = 0; i < count; i++)
            {
                var currentNuid = nuid.GetNext();
                
                //HashSet.Add returns false if the set already contains the item
                if (nuids.Add(currentNuid))
                    continue;
                
                _outputHelper.WriteLine($"Duplicate Nuid {currentNuid}");
                Assert.True(false, "Duplicate Nuid detected");
            }
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
