// Copyright 2019-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Internals
{
    public static class JsonUtils
    {
        public static readonly JSONNode MinusOne = new JSONNumber(-1);

        public static int AsIntOrMinus1(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsInt : -1;
        }

        public static long AsLongOrZero(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : 0;
        }

        public static long AsLongOrMinus1(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsLong : -1;
        }

        public static ulong AsUlongOrZero(JSONNode node, string field)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsUlong : 0;
        }

        public static ulong AsUlongOr(JSONNode node, string field, ulong dflt)
        {
            JSONNode possible = node[field];
            return possible.IsNumber ? possible.AsUlong : dflt;
        }

        public static Duration AsDuration(JSONNode node, string field, Duration dflt)
        {
            if (dflt == null)
            {
                long l = node.GetValueOrDefault(field, long.MinValue).AsLong;
                return l == long.MinValue ? null : Duration.OfNanos(l);
            }
            return Duration.OfNanos(node.GetValueOrDefault(field, dflt.Nanos).AsLong);
        }

        public static List<string> StringList(JSONNode node, string field)
        {
            List<string> list = new List<string>();
            foreach (var child in node[field].Children)
            {
                list.Add(child.Value);
            }
           
            return list;
        }

        public static List<Duration> DurationList(JSONNode node, string field, bool nullIfEmpty = false)
        {
            List<Duration> list = new List<Duration>();
            foreach (var child in node[field].Children)
            {
                list.Add(Duration.OfNanos(child.AsLong));
            }
           
            return list.Count == 0 && nullIfEmpty ? null : list;
        }

        public static List<T> ListOf<T>(JSONNode node, string field, Func<JSONNode, T> provider, bool nullIfEmpty = false)
        {
            List<T> list = new List<T>();
            foreach (var child in node[field].Children)
            {
                list.Add(provider.Invoke(child));
            }
          
            return list.Count == 0 && nullIfEmpty ? null : list;
        }

        public static List<string> OptionalStringList(JSONNode node, string field)
        {
            List<string> list = StringList(node, field);
            return list.Count == 0 ? null : list;
        }

        [Obsolete("Method name replaced with proper spelling, 'StringStringDictionary'")]
        public static IDictionary<string, string> StringStringDictionay(JSONNode node, string field)
        {
            return StringStringDictionary(node, field, false);
        }

        public static IDictionary<string, string> StringStringDictionary(JSONNode node, string field, bool nullIfEmpty = false)
        {
            IDictionary<string, string> temp = new Dictionary<string, string>();
            JSONNode meta = node[field];
            foreach (string key in meta.Keys)
            {
                temp[key] = meta[key];
            }
            return temp.Count == 0 && nullIfEmpty ? null : temp;
        }

        public static MsgHeader AsHeaders(JSONNode node, string field)
        {
            MsgHeader h = new MsgHeader();
            JSONNode hNode = node[field];
            if (hNode != null)
            {
                foreach (string key in hNode.Keys)
                {
                    foreach (var val in hNode[key].Values)
                    {
                        h.Add(key, val);
                    }
                }
            }
            return h;
        }
        
        public static byte[] AsByteArrayFromBase64(JSONNode node) {
            return string.IsNullOrWhiteSpace(node.Value) ? null : Convert.FromBase64String(node.Value);
        }
        
        public static DateTime AsDate(JSONNode node)
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

        public static string ToString(DateTime dt)
        {
            // Assume MinValue is Unset
            return dt.Equals(DateTime.MinValue) ? null : UnsafeToString(dt);
        }
        
        public static string UnsafeToString(DateTime dt)
        {
            return dt.ToUniversalTime().ToString("O");
        }

        public static JSONArray ToArray(List<string> list)
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

        public static string ToKey(Type type)
        {
            return "\"" + type.Name + "\":";
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
                    return name + serializable.ToJsonNode();
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

        public static void AddFieldEvenEmpty(JSONObject o, string field, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                o[field] = "";
            }
            else
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, JsonSerializable value)
        {
            if (value != null)
            {
                o[field] = value.ToJsonNode();
            }
        }

        public static void AddField(JSONObject o, string field, JSONNode value)
        {
            if (value != null)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string fname, IDictionary<string, string> dictionary) {
            if (dictionary != null && dictionary.Count > 0) {
                JSONObject d = new JSONObject();
                foreach (string key in dictionary.Keys)
                {
                    d[key] = dictionary[key];
                }
                o[fname] = d;
            }
        }

        public static void AddField(JSONObject o, string field, DateTime? value)
        {
            if (value != null && value != DateTime.MinValue)
            {
                o[field] = UnsafeToString(value.Value);
            }
        }

        public static void AddField(JSONObject o, string field, int value)
        {
            if (value >= 0)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, int? value)
        {
            if (value != null && value >= 0)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, long value)
        {
            if (value >= 0)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, long? value)
        {
            if (value != null && value >= 0)
            {
                o[field] = value;
            }
        }

        public static void AddFieldWhenGtZero(JSONObject o, string field, int value)
        {
            if (value > 0)
            {
                o[field] = value;
            }
        }

        public static void AddFieldWhenGtZero(JSONObject o, string field, int? value)
        {
            if (value != null && value > 0)
            {
                o[field] = value;
            }
        }

        public static void AddFieldWhenGtZero(JSONObject o, string field, long value)
        {
            if (value > 0)
            {
                o[field] = value;
            }
        }

        public static void AddFieldWhenGtZero(JSONObject o, string field, long? value)
        {
            if (value != null && value > 0)
            {
                o[field] = value;
            }
        }

        public static void AddFieldWhenGteMinusOne(JSONObject o, string field, long? value)
        {
            if (value != null && value >= -1)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, Duration value)
        {
            if (value != null && value.IsPositive())
            {
                o[field] = value.Nanos;
            }
        }

        public static void AddField(JSONObject o, string field, ulong value)
        {
            o[field] = value;
        }

        public static void AddFieldWhenGreaterThan(JSONObject o, string field, ulong value, ulong gt)
        {
            if (value > gt)
            {
                o[field] = value;
            }
        }

        public static void AddField(JSONObject o, string field, ulong? value)
        {
            if (value != null)
            {
                o[field] = value;
            }
        }
 
        public static void AddField(JSONObject o, string field, bool value)
        {
            if (value)
            {
                o[field] = true;
            }
        }
 
        public static void AddField(JSONObject o, string field, bool? value)
        {
            if (value != null && value == true)
            {
                o[field] = true;
            }
        }

        public static void AddField(JSONObject o, string field, IList<Duration> values)
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

        public static void AddField(JSONObject o, string field, string[] values)
        {
            if (values != null && values.Length > 0)
            {
                JSONArray ja = new JSONArray();
                foreach (string v in values)
                {
                    ja.Add(v);
                }
                o[field] = ja;
            }
        }

        public static void AddField(JSONObject o, string field, IList<string> values)
        {
            if (values != null && values.Count > 0)
            {
                JSONArray ja = new JSONArray();
                foreach (string v in values)
                {
                    ja.Add(v);
                }
                o[field] = ja;
            }
        }

        public static void AddField<T>(JSONObject o, string field, IList<T> values) where T : JsonSerializable
        {
            if (values != null && values.Count > 0)
            {
                JSONArray ja = new JSONArray();
                foreach (JsonSerializable v in values)
                {
                    ja.Add(v.ToJsonNode());
                }
                o[field] = ja;
            }
        }

        public static void AddField(JSONObject o, string field, MsgHeader headers)
        {
            if (headers != null && headers.Count > 0)
            {
                JSONObject h = new JSONObject();
                foreach (string key in headers.Keys)
                {
                    AddField(h, key, headers.GetValues(key));
                }
                o[field] = h;
            }
        }
    }
}
