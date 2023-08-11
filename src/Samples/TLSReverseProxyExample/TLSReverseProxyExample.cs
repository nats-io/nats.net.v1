using NATS.Client;
using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace NATSExamples
{
    /// <summary>
    /// This example shows how to use a TLS-Terminating proxy with the NATs .NET client
    /// 
    /// This example is not production hardened
    /// 
    /// You can create a TLS Terminating proxy using Stunnel. 
    /// 
    /// </summary>
    internal static class TlsVariationsExample
    {
        // 8444 is a port where the Terminating Proxy is listening
        static readonly string Url = "nats://192.168.1.108:8444";
        // This is unsafe and assumes all certificates are good. 
        private static bool verifyServerCert(object sender,
            X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            return true;
            
        }

        public static void Main(string[] args)
        {

            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = Url;
            opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
            opts.TCPConnection = new CustomTCPConnection();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {

                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
            }
        }
    }
}


