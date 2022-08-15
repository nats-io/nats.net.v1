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
        
        internal static Error OptionalInstance(JSONNode error)
        {
            return error.Count == 0 ? null : new Error(error);
        }

        internal Error(JSONNode node)
        {
            Code = JsonUtils.AsIntOrMinus1(node, ApiConstants.Code);
            ApiErrorCode = JsonUtils.AsIntOrMinus1(node, ApiConstants.ErrCode);
            string temp = node[ApiConstants.Description];
            Desc = temp ?? "Unknown JetStream Error";
        }

        internal Error(int code, int apiErrorCode, string desc) {
            Code = code;
            ApiErrorCode = apiErrorCode;
            Desc = desc;
        }

        public override string ToString()
        {
            if (ApiErrorCode == NOT_SET) {
                if (Code == NOT_SET) {
                    return Desc;
                }
                return $"{Desc} ({Code})";
            }

            if (Code == NOT_SET) {
                return Desc;
            }

            return $"{Desc} [{ApiErrorCode}]";
        }

        internal static Error Convert(MsgStatus status) {
            switch (status.Code) {
                case 404:
                    return JsNoMessageFoundErr;
                case 408:
                    return JsBadRequestErr;
            }
            return new Error(status.Code, NOT_SET, status.Message);
        }

        internal static readonly Error JsBadRequestErr = new Error(400, 10003, "bad request");
        internal static readonly Error JsNoMessageFoundErr = new Error(404, 10037, "no message found");
    }
}
