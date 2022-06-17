using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using NATS.Client;

namespace NATSExamples
{
    /// <summary>
    /// This example is a shell for self handling OCSP and TLS for that matter
    ///
    /// opts.AddCertificate(cert) is required for standard TLS when you don't provide a
    /// opts.TLSRemoteCertificationValidationCallback
    ///
    /// If you provide a callback, you can do whatever you like, including checking the
    /// certificate and chain.
    /// 
    /// The client.pfx found in this project is based on the
    /// client-key.pem and the client-cert.pem found here:
    ///     https://github.com/nats-io/nats-server/tree/main/test/configs/certs/ocsp
    /// It was made following the instructions here:
    ///     https://github.com/nats-io/nats.net/blob/master/README.md#tls
    /// </summary>
    internal static class TlsOcspExample
    {
        private static bool VerifyServerCert(object sender,
            X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            Console.WriteLine();

            if (certificate is X509Certificate2 cert2)
            {
                Console.WriteLine("X509Certificate2 Extensions");
                foreach (X509Extension ext in cert2.Extensions)
                {
                    Console.WriteLine("  " + ext.GetType().Name 
                                      + " | " + ext.Oid.FriendlyName 
                                      + " | " + ext.Critical 
                                      + " | " + Encoding.UTF8.GetString(ext.RawData));
                }
                Console.WriteLine();
            }

            Console.WriteLine("sslPolicyErrors");
            Console.WriteLine("  " + sslPolicyErrors);
            Console.WriteLine();
            
            if (chain != null)
            {
                Console.WriteLine("X509Chain Statuses");
                foreach (X509ChainStatus cs in chain.ChainStatus)
                {
                    Console.WriteLine("  " + cs.Status + " | " + cs.StatusInformation);
                }
                Console.WriteLine();
            }

            return true; // true if the cert is okay, false if it not
        }

        public static void Main(string[] args)
        {
            string url = "tls://127.0.0.1:56324";
            X509Certificate2 cert = new X509Certificate2("client.pfx", "password");

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;
            opts.Secure = true;
            
            // either this
            opts.AddCertificate(cert);
            
            // or you have to do it all yourself
            opts.TLSRemoteCertificationValidationCallback = VerifyServerCert;

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
