using System;
using System.Net;
using System.Net.Sockets;

namespace NATS.Client.Extensions
{
    public static class UriExtensions
    {
        public static AddressFamily? GetInterNetworkAddressFamily(this Uri uri)
        {
            if(uri.HostNameType != UriHostNameType.IPv4 && uri.HostNameType != UriHostNameType.IPv6)
                return null;

            if (IPAddress.TryParse(uri.Host, out var ipAddress) && ipAddress?.AddressFamily == AddressFamily.InterNetwork || ipAddress?.AddressFamily == AddressFamily.InterNetworkV6)
                return ipAddress.AddressFamily;

            return null;
        }
    }
}