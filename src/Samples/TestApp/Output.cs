using System;
using NATS.Client.Internals.SimpleJSON;

namespace NATSExamples
{
    public static class Output
    {
        public static readonly object AppendLock = new object();

        public static bool ShowWork = false;
        public static bool ShowControl = true;
        public static bool ShowDebug = false;
        
        public static void Work(String label, String s) {
            if (ShowWork) {
                Append("WORK", label, s);
            }
        }

        public static void ControlMessage(String label, String s) {
            if (ShowControl) {
                Append("CTRL", label, s);
            }
        }

        public static void Debug(String label, String s)
        {
            if (ShowDebug) {
                Append("DBUG", label, s);
            }
        }

        const string NlIndent = "\n    ";
        public static void Append(string area, string label, string s) {
            lock (AppendLock)
            {
                string alabel = " | " + area + " | " + label; 
                if (s.Contains("\n"))
                {
                    String timeLabel = Time() + alabel;
                    Console.Write(timeLabel);
                    if (!s.StartsWith("\n"))
                    {
                        Console.Write(" | ");
                    }
                    Console.Write(s.Replace("\n", NlIndent));
                }
                else
                {
                    Console.Write(Time());
                    Console.Write(alabel);
                    Console.Write(" | ");
                    Console.Write(s);
                }

                Console.WriteLine();
            }
        }

        public static string Time()
        {
            return $"{DateTimeOffset.Now.ToUnixTimeMilliseconds()}".Substring(7);
        }
        
        public static String FN = "\n  ";
        public static String FBN = "{\n  ";
        public static String Formatted(JSONNode j) {
            return j.GetType().Name + j.ToString()
                .Replace("{\"", FBN + "\"").Replace(",", "," + FN);
        }

        public static String Formatted(Object o) {
            return Formatted(o.ToString());
        }

        public static String Formatted(String s) {
            return s.Replace("{", FBN).Replace(", ", "," + FN);
        }
    }
}