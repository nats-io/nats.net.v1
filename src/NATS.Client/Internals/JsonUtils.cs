using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Internals
{
    internal static class JsonUtils
    {
        internal static readonly JSONNode MinusOne = new JSONNumber(-1);

        internal static int AsIntOrMinus1(JSONNode node, String field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsInt : -1;
        }

        internal static long AsLongOrZero(JSONNode node, String field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : 0;
        }

        internal static long AsLongOrMinus1(JSONNode node, String field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : -1;
        }

        internal static ulong AsUlongOrZero(JSONNode node, String field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsUlong : 0;
        }

        internal static Duration AsDuration(JSONNode node, String field, Duration dflt)
        {
            return Duration.OfNanos(node.GetValueOrDefault(field, dflt.Nanos).AsLong);
        }

        internal static List<string> StringList(JSONNode node, String field)
        {
            List<string> list = new List<string>();
            foreach (var child in node[field].Children)
            {
                list.Add(child.Value);
            }
           
            return list;
        }

        internal static List<string> OptionalStringList(JSONNode node, String field)
        {
            List<string> list = StringList(node, field);
            return list.Count == 0 ? null : list;
        }
        
        internal static byte[] AsByteArrayFromBase64(JSONNode node) {
            return string.IsNullOrWhiteSpace(node.Value) ? null : Convert.FromBase64String(node.Value);
        }
        
        internal static DateTime AsDate(JSONNode node)
        {
            try
            {
                return DateTime.Parse(node.Value).ToUniversalTime();
            }
            catch (Exception)
            {
                return DateTime.MinValue;
            }
        }
        
        internal static string ToString(DateTime dt)
        {
            // Assume MinValue is Unset
            return dt.Equals(DateTime.MinValue) ? null : UnsafeToString(dt);
        }
        
        internal static string UnsafeToString(DateTime dt)
        {
            return dt.ToUniversalTime().ToString("O");
        }

        internal static JSONArray ToArray(List<string> list)
        {
            JSONArray arr = new JSONArray();
            if (list == null)
            {
                return arr;
            }
            foreach (var s in list)
            {
                arr.Add(null, new JSONString(s));
            }
            return arr;
        }
 
        public static byte[] SimpleMessageBody(string name, long value)
        {
            return Encoding.ASCII.GetBytes("{\"" + name + "\":" + value + "}");
        }

        public static byte[] SimpleMessageBody(string name, ulong value)
        {
            return Encoding.ASCII.GetBytes("{\"" + name + "\":" + value + "}");
        }

        public static byte[] SimpleMessageBody(string name, string value)
        {
            return Encoding.ASCII.GetBytes("{\"" + name + "\":\"" + value + "\"}");
        }
        
        public static byte[] Serialize(JSONNode node)
        {
            return Encoding.ASCII.GetBytes(node.ToString());
        }

        public static string ObjectString(string name, object o) {
            switch (o)
            {
                case null:
                    return name + "=null";
                case JsonSerializable serializable:
                    return name + serializable.ToJsonNode().ToString();
                default:
                    return o.ToString();
            }
        }

        public static void AddField(JSONObject o, string field, string value)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, DateTime? value)
        {
            DateTime dt = value.GetValueOrDefault(DateTime.MinValue);
            if (!dt.Equals(DateTime.MinValue))
            {
                o[field] = UnsafeToString(dt);
            }
        }

        public static void AddField(JSONObject o, string field, long? value)
        {
            long l = value.GetValueOrDefault(-1);
            if (l >= 0)
            {
                o[field] = l;
            }
        }

        public static void AddField(JSONObject o, string field, ulong? value)
        {
            ulong u = value.GetValueOrDefault(0);
            if (u > 0)
            {
                o[field] = u;
            }
        }
 
        public static void AddField(JSONObject o, string field, bool? value)
        {
            bool b = value.GetValueOrDefault(false);
            if (b)
            {
                o[field] = b;
            }
        }
    }
}