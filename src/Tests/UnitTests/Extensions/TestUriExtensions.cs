using System;
using System.Net.Sockets;
using NATS.Client.Extensions;
using Xunit;

namespace UnitTests.Extensions
{
    public class TestUriExtensions
    {
        [Theory]
        [InlineData("nats://127.0.0.1:4222", AddressFamily.InterNetwork)]
        [InlineData("nats://[::1]:4222", AddressFamily.InterNetworkV6)]
        [InlineData("nats://localhost:4222", null)]
        [InlineData("http://nats.io", null)]
        [InlineData("c:/temp.txt", null)]
        public void GetInterNetworkAddressFamily(string address, AddressFamily? expected)
        {
            var r = new Uri(address).GetInterNetworkAddressFamily();
            
            Assert.Equal(expected, r);
        }
    }
}