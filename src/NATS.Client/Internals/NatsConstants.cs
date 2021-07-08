using System.Text;

namespace NATS.Client.Internals
{
    internal static class NatsConstants
    {
        internal const string HeaderVersion = "NATS/1.0";
        internal const string HeaderVersionBytesPlusCrlf = "NATS/1.0\r\n";
        internal static readonly byte[] HeaderVersionBytes = Encoding.ASCII.GetBytes(HeaderVersion);
        internal static readonly int HeaderVersionBytesLen = HeaderVersionBytes.Length;
        internal static readonly int MinimalValidHeaderLen = HeaderVersionBytesLen + 2; // 2 is crlf

        /// <summary>
        /// No Responders Status code, 503.
        /// </summary>
        internal const int NoRespondersCode = 503;

        /// <summary>
        /// Not Found Status code, 404.
        /// </summary>
        internal const int NotFoundCode = 404;

        internal const string InvalidHeaderVersion = "Invalid header version";
        
        internal const string InvalidHeaderComposition = "Invalid header composition";
        
        internal const string InvalidHeaderStatusCode = "Invalid header status code";
        
        internal const string SerializedHeaderCannotBeNullOrEmpty = "Serialized header cannot be null or empty.";
        
        internal const string Empty = "";

        internal const byte Sp = (byte)' ';
        internal const byte Colon = (byte)':';
        internal const byte Cr = (byte)'\r';
        internal const byte Lf = (byte)'\n';
    }
}