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
        
        internal const int FlowOrHeartbeatStatusCode = 100;
        internal const int NoRespondersCode = 503;
        internal const int BadRequestCode = 400;
        internal const int NotFoundCode = 404;
        internal const int RequestTimeoutCode = 408;
        internal const int ConflictCode = 409;
        
        internal const string InvalidHeaderVersion = "Invalid header version";
        
        internal const string InvalidHeaderComposition = "Invalid header composition";
        
        internal const string InvalidHeaderStatusCode = "Invalid header status code";
        
        internal const string SerializedHeaderCannotBeNullOrEmpty = "Serialized header cannot be null or empty.";
        
        internal const string Empty = "";
        internal const char Dot = '.';

        internal const byte Sp = (byte)' ';
        internal const byte Colon = (byte)':';
        internal const byte Cr = (byte)'\r';
        internal const byte Lf = (byte)'\n';
    }
}