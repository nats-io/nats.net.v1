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

        internal static int AsIntOrMinus1(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsInt : -1;
        }

        internal static long AsLongOrZero(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : 0;
        }

        internal static long AsLongOrMinus1(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : -1;
        }

        internal static ulong AsUlongOrZero(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsUlong : 0;
        }

        internal static Duration AsDuration(JSONNode node, string field, Duration dflt)
        {
            if (dflt == null)
            {
                long l = node.GetValueOrDefault(field, long.MinValue).AsLong;
                return l == long.MinValue ? null : Duration.OfNanos(l);
            }
            return Duration.OfNanos(node.GetValueOrDefault(field, dflt.Nanos).AsLong);
        }

        internal static List<string> StringList(JSONNode node, string field)
        {
            List<string> list = new List<string>();
            foreach (var child in node[field].Children)
            {
                list.Add(child.Value);
            }
           
            return list;
        }

        internal static List<Duration> DurationList(JSONNode node, string field)
        {
            List<Duration> list = new List<Duration>();
            foreach (var child in node[field].Children)
            {
                list.Add(Duration.OfNanos(child.AsLong));
            }
           
            return list;
        }

        internal static List<string> OptionalStringList(JSONNode node, string field)
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
 
        internal static byte[] SimpleMessageBody(string name, long value)
        {
            return Encoding.ASCII.GetBytes("{\"" + name + "\":" + value + "}");
        }

        internal static byte[] SimpleMessageBody(string name, ulong value)
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
                    return name + serializable.ToJsonNode();
                default:
                    return o.ToString();
            }
        }

        internal static void AddField(JSONObject o, string field, string value)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                o[field] = value;
            }
        }

        internal static void AddField(JSONObject o, string field, DateTime? value)
        {
            if (value != null)
            {
                o[field] = UnsafeToString(value.Value);
            }
        }

        internal static void AddField(JSONObject o, string field, long? value)
        {
            if (value != null && value >= 0)
            {
                o[field] = value;
            }
        }

        internal static void AddField(JSONObject o, string field, Duration value)
        {
            if (value != null)
            {
                o[field] = value.Nanos;
            }
        }

        internal static void AddField(JSONObject o, string field, ulong? value)
        {
            if (value != null)
            {
                o[field] = value;
            }
        }
 
        internal static void AddField(JSONObject o, string field, bool? value)
        {
            if (value != null && value == true)
            {
                o[field] = true;
            }
        }

        internal static void AddField(JSONObject o, string field, IList<Duration> values)
        {
            if (values != null && values.Count > 0)
            {
                JSONArray ja = new JSONArray();
                foreach (Duration d in values)
                {
                    ja.Add(d.Nanos);
                }
                o[field] = ja;
            }
        }
    }
}
