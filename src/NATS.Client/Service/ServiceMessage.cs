// Copyright 2022 The NATS Authors
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

using System.Text;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceMessage
    {
        public const string NatsServiceError = "Nats-Service-Error";
        public const string NatsServiceErrorCode = "Nats-Service-Error-Code";

        public static void Reply(IConnection conn, Msg request, byte[] data) {
            conn.Publish(new Msg(request.Reply, null, null, data));
        }

        public static void Reply(IConnection conn, Msg request, string data) {
            conn.Publish(new Msg(request.Reply, null, null, Encoding.UTF8.GetBytes(data)));
        }

        public static void Reply(IConnection conn, Msg request, byte[] data, MsgHeader headers)
        {
            conn.Publish(new Msg(request.Reply, null, headers, data));
        }

        public static void Reply(IConnection conn, Msg request, string data, MsgHeader headers)
        {
            conn.Publish(new Msg(request.Reply, null, headers, Encoding.UTF8.GetBytes(data)));
        }

        public static void ReplyStandardError(IConnection conn, Msg request, string errorMessage, int errorCode)
        {
            conn.Publish(new Msg(request.Reply, null, 
                new MsgHeader 
                {
                    [NatsServiceError] = errorMessage,
                    [NatsServiceErrorCode] = $"{errorCode}" 
                }, 
                null));
        }    
    }
}
