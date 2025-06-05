using System;
using System.Collections.Generic;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NATS.Client;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities.Collections;

namespace NATSExamples
{
    /// <summary>
    /// This example shows various ways to handle TLS, including manual verification and
    /// OCSP revocation checking.
    ///
    /// Revocation checking requires: opts.CheckCertificateRevocation = true
    /// Manual Verification requires: opts.TLSRemoteCertificationValidationCallback = ...
    ///
    /// If you provide a manual callback, you can do whatever you like, including checking
    /// the certificate and chain.
    /// 
    /// X509 Certificate 2
    ///     https://docs.microsoft.com/en-us/dotnet/api/system.security.cryptography.x509certificates.x509certificate2?view=net-6.0
    /// X509 Extension Class:
    ///     https://docs.microsoft.com/en-us/dotnet/api/system.security.cryptography.x509certificates.x509extension?view=net-6.0
    ///
    /// BouncyCastle is an industry standard crypto library https://www.bouncycastle.org/index.html
    /// It might provide functionality that will help handling the certificate and chain.
    /// 
    /// </summary>
    internal static class TlsVariationsExample
    {
        static readonly string Url = "tls://localhost:4222";

        public static void Main(string[] args)
        {
            var opts = RevocationCheckingOptions();
            // var opts = ManualVerificationOptions();
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

        private static Options RevocationCheckingOptions()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = Url;
            opts.Secure = true;
            X509Certificate2 cert = new X509Certificate2("client.pfx", "password");
            opts.AddCertificate(cert);
            opts.CheckCertificateRevocation = true;
            return opts;
        }

        private static Options ManualVerificationOptions()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = Url;
            opts.Secure = true;
            opts.TLSRemoteCertificationValidationCallback = ManualServerCertVerification;
            return opts;
        }

        private static bool ManualServerCertVerification(object sender,
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

            // ------------------------------------------------------------------------------------------
            // BouncyCastle
            // ------------------------------------------------------------------------------------------
            Org.BouncyCastle.X509.X509Certificate bcX509 = DotNetUtilities.FromX509Certificate(certificate);
            Console.WriteLine("BouncyCastle X509Certificate");
            Console.WriteLine("  " + bcX509.CertificateStructure);
            Console.WriteLine("  " + bcX509.IsValidNow);
            Console.WriteLine("  " + bcX509.NotBefore);
            Console.WriteLine("  " + bcX509.NotAfter);
            Console.WriteLine("  " + bcX509.SigAlgName);
            Console.WriteLine("  " + bcX509.SigAlgOid);
            if (bcX509.GetExtendedKeyUsage().Count > 0)
            {
                Console.WriteLine("  Extended Key Usage");
                foreach (var eku in bcX509.GetExtendedKeyUsage())
                {
                    Console.WriteLine("    " + eku);
                }
            }

            void InspectExtension(ISet<string> extSet, string label)
            {
                if (extSet.Count > 0)
                {
                    Console.WriteLine("  " + label);
                    foreach (string oid in extSet)
                    {
                        try
                        {
                            Asn1OctetString asn = bcX509.GetExtensionValue(new DerObjectIdentifier(oid));
                            Console.WriteLine("    Oid: " + oid + " | Asn: " + asn);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }
                }
            }

            InspectExtension(bcX509.GetNonCriticalExtensionOids(), "Non Critical Extensions");
            InspectExtension(bcX509.GetCriticalExtensionOids(), "Critical Extensions");

            return true; // true if the cert is okay, false if it not
        }
    }
}
