using NATS.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using static NATS.Client.Defaults;


namespace NATSExamples
{


        /// <summary>
        /// Convenience class representing the TCP connection to prevent 
        /// managing two variables throughout the NATs client code.
        /// 
        /// This "Custom" implementation just makes the connection TLS after opening it. 
        /// </summary>
        public class CustomTCPConnection : Connection.TCPConnection
        {

            public override void open(Srv s, Options options)
            {
                base.open(s, options);
                base.makeTLS();
            } 
        }
    }

