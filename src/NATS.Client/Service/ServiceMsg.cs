// Copyright 2023 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// Service Message is service specific object that exposes the service relevant parts of a NATS Message.
    /// </summary>
    public class ServiceMsg
    {
        /// <summary>
        /// Standard header name used to report the text of an error
        /// </summary>
        public const string NatsServiceError = "Nats-Service-Error";
        
        /// <summary>
        /// Standard header name used to report the code of an error
        /// </summary>
        public const string NatsServiceErrorCode = "Nats-Service-Error-Code";
        
        private readonly Msg msg;

        /// <value>The subject that this message was sent to.</value>
        public string Subject => msg.Subject;

        /// <value>The subject the application is expected to send a reply message on.</value>
        public string Reply => msg.Reply;

        /// <value>Whether there are headers.</value>
        public bool HasHeaders => msg.HasHeaders;

        /// <value>The headers object for the message.</value>
        public MsgHeader Header => msg.Header;

        /// <value>The data from the message.</value>
        public byte[] Data => msg.Data;
        
        internal ServiceMsg(Msg msg)
        {
            this.msg = msg;
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a byte array </param>
        public void Respond(IConnection conn, byte[] response)
        {
            conn.Publish(new Msg(msg.Reply, null, null, response));
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a string</param>
        public void Respond(IConnection conn, string response)
        {
            conn.Publish(new Msg(msg.Reply, null, null, Encoding.UTF8.GetBytes(response)));
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a <see cref="JsonSerializable"/> object</param>
        public void Respond(IConnection conn, JsonSerializable response) 
        {
            conn.Publish(new Msg(msg.Reply, null, null, response.Serialize()));
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a <see cref="JSONNode"/> object</param>
        public void Respond(IConnection conn, JSONNode response) 
        {
            conn.Publish(new Msg(msg.Reply, null, null, Encoding.UTF8.GetBytes(response.ToString())));
        }

        /// <summary>
        /// Respond to a service request message with a response and custom headers.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a byte array</param>
        /// <param name="headers">the custom headers</param>
        public void Respond(IConnection conn, byte[] response, MsgHeader headers)
        {
            conn.Publish(new Msg(msg.Reply, null, headers, response));
        }

        /// <summary>
        /// Respond to a service request message with a response and custom headers.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a string</param>
        /// <param name="headers">the custom headers</param>
        public void Respond(IConnection conn, string response, MsgHeader headers)
        {
            conn.Publish(new Msg(msg.Reply, null, headers, Encoding.UTF8.GetBytes(response)));
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a <see cref="JsonSerializable"/> object</param>
        /// <param name="headers">the custom headers</param>
        public void Respond(IConnection conn, JsonSerializable response, MsgHeader headers) 
        {
            conn.Publish(new Msg(msg.Reply, null, headers, response.Serialize()));
        }

        /// <summary>
        /// Respond to a service request message.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="response">the response payload in the form of a <see cref="JSONNode"/> object</param>
        /// <param name="headers">the custom headers</param>
        public void Respond(IConnection conn, JSONNode response, MsgHeader headers) 
        {
            conn.Publish(new Msg(msg.Reply, null, headers, Encoding.UTF8.GetBytes(response.ToString())));
        }

        /// <summary>
        /// Respond to a service request message with a standard error.
        /// </summary>
        /// <param name="conn">the NATS connection</param>
        /// <param name="errorText">the error message text</param>
        /// <param name="errorCode">the error message code</param>
        public void RespondStandardError(IConnection conn, string errorText, int errorCode)
        {
            conn.Publish(new Msg(msg.Reply, null, 
                new MsgHeader 
                {
                    [NatsServiceError] = errorText,
                    [NatsServiceErrorCode] = $"{errorCode}" 
                }, 
                null));
        }    
    }
}
