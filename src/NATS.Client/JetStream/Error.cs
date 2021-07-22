// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class Error
    {
        public const int NOT_SET = -1;

        public int Code { get; }
        public int ApiErrorCode { get; }
        public string Desc { get; }
        private JSONNode _node;
        
        internal static Error OptionalInstance(JSONNode error)
        {
            return error.Count == 0 ? null : new Error(error);
        }

        internal Error(JSONNode node)
        {
            _node = node;
            Code = JsonUtils.AsIntOrMinus1(node, ApiConstants.Code);
            ApiErrorCode = JsonUtils.AsIntOrMinus1(node, ApiConstants.ErrCode);
            string temp = node[ApiConstants.Description];
            Desc = temp ?? "Unknown JetStream Error";
        }

        public override string ToString()
        {
            if (Code == NOT_SET) 
            {
                if (ApiErrorCode == NOT_SET) 
                {
                    return Desc;
                }
                return $"{Desc} [{ApiErrorCode}]";
            }

            if (ApiErrorCode == NOT_SET) 
            {
                return $"{Desc} ({Code})";
            }

            return $"{Desc} ({Code}) [{ApiErrorCode}]";
        }
    }
}
