using System;
using System.IO;

namespace UnitTests
{
    public class TestBase
    {
        internal const string Empty     = "";
        internal const string Plain     = "plain";
        internal const string HasSpace  = "has space";
        internal const string HasDash   = "has-dash";
        internal const string HasDot    = "has.dot";
        internal const string HasStar   = "has*star";
        internal const string HasGt     = "has>gt";
        internal const string HasDollar = "has$dollar";
        internal const string HasTab    = "has\tgt";

        internal static string ReadDataFile(string name)
        {
            string path = Directory.GetCurrentDirectory();
            string fileSpec = Path.Combine(path, "..", "..", "..", "Jetstream", "Api", "Data", name);
            return File.ReadAllText(fileSpec);
        }

        internal static DateTime AsDateTime(string dtString)
        {
            return DateTime.Parse(dtString).ToUniversalTime();
        }
        
        // ----------------------------------------------------------------------------------------------------
        // data makers
        // ----------------------------------------------------------------------------------------------------
        internal const string STREAM = "stream";
        internal const string MIRROR = "mirror";
        internal const string SOURCE = "source";
        internal const string SUBJECT = "subject";
        internal const string SUBJECT_STAR = SUBJECT + ".*";
        internal const string SUBJECT_GT = SUBJECT + ".>";
        internal const string QUEUE = "queue";
        internal const string DURABLE = "durable";
        internal const string DELIVER = "deliver";
        internal const string MESSAGE_ID = "mid";
        internal const string DATA = "data";

        internal static string Stream(int seq) {
            return STREAM + "-" + seq;
        }

        internal static string Mirror(int seq) {
            return MIRROR + "-" + seq;
        }

        internal static string Source(int seq) {
            return SOURCE + "-" + seq;
        }

        internal static string Subject(int seq) {
            return SUBJECT + "-" + seq;
        }

        internal static string Queue(int seq) {
            return QUEUE + "-" + seq;
        }

        internal static string Durable(int seq) {
            return DURABLE + "-" + seq;
        }

        internal static string Durable(string vary, int seq) {
            return DURABLE + "-" + vary + "-" + seq;
        }

        internal static string Deliver(int seq) {
            return DELIVER + "-" + seq;
        }

        internal static string MessageId(int seq) {
            return MESSAGE_ID + "-" + seq;
        }

        internal static string Data(int seq) {
            return DATA + "-" + seq;
        }
    }
}