using System;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
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
    ///
    /// X509 Extension Class:
    ///     https://docs.microsoft.com/en-us/dotnet/api/system.security.cryptography.x509certificates.x509extension?view=net-6.0
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
                Console.WriteLine("X509Certificate2");
                try
                {
                    byte[] rawdata = cert2.RawData;
                    Console.WriteLine("  Content Type: " + X509Certificate2.GetCertContentType(rawdata));
                    Console.WriteLine("  Friendly Name: " + cert2.FriendlyName);
                    Console.WriteLine("  Certificate Verified?: " + cert2.Verify());
                    Console.WriteLine("  Simple Name: " + cert2.GetNameInfo(X509NameType.SimpleName,true));
                    Console.WriteLine("  Signature Algorithm: " + cert2.SignatureAlgorithm.FriendlyName);
                    // Console.WriteLine("  Public Key: " + cert2.PublicKey.Key.ToXmlString(false));
                    Console.WriteLine("  Certificate Archived?: " + cert2.Archived);
                    Console.WriteLine("  Length of Raw Data: " + cert2.RawData.Length);
                }
                catch (CryptographicException)
                {
                    Console.WriteLine("Information could not be written out for this certificate.");
                }
                Console.WriteLine();

                Console.WriteLine("X509Certificate2 Extensions");
                foreach (X509Extension ext in cert2.Extensions)
                {
                    Console.WriteLine("  " + ext.GetType().Name 
                                      + "\n    Oid: " + ext.Oid.FriendlyName 
                                      + "\n    Critical: " + ext.Critical 
                                      + "\n    Raw Len: " + ext.RawData.Length);
                    
                    if (ext is X509BasicConstraintsExtension bcExt)
                    {
                        Console.WriteLine("    CA: " + bcExt.CertificateAuthority);
                        Console.WriteLine("    HPLC: " + bcExt.HasPathLengthConstraint);
                        Console.WriteLine("    PLC: " + bcExt.PathLengthConstraint);
                    }
                    else if (ext is X509KeyUsageExtension kuExt)
                    {
                        Console.WriteLine("    Usages: " + kuExt.KeyUsages);
                    }
                    else if (ext is X509EnhancedKeyUsageExtension ekuExt)
                    {
                        if (ekuExt.EnhancedKeyUsages.Count > 0)
                        {
                            Console.WriteLine("    Enhanced Key Usages");
                            foreach (Oid oid in ekuExt.EnhancedKeyUsages)
                            {
                                Console.WriteLine("      " + oid.FriendlyName);
                            }
                        }
                    }
                    else if (ext is X509SubjectKeyIdentifierExtension skiExt)
                    {
                        Console.WriteLine("    Subject Key Identifier: " + skiExt.SubjectKeyIdentifier);
                        
                    }
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
            string url = "tls://127.0.0.1:59833";
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
