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

using System;

namespace NATS.Client.Service.Contexts
{
    internal class ServiceContext : Context
    {
        private readonly EventHandler<MsgHandlerEventArgs> _serviceMsgHandler;
            
        internal ServiceContext(IConnection conn, string subject, 
            StatsResponse statsResponse, EventHandler<MsgHandlerEventArgs> serviceMsgHandler) 
            : base(conn, subject, statsResponse, true)
        {
            _serviceMsgHandler = serviceMsgHandler;
        }

        protected override void SubOnMessage(object sender, MsgHandlerEventArgs args)
        {
            _serviceMsgHandler.Invoke(sender, args);
        }
    }
}