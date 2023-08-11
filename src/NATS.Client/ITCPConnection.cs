using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace NATS.Client
{
    public interface ITCPConnection : IDisposable 
    {
        bool Connected { get; }
        bool DataAvailable { get; }
        int SendTimeout { set; }
        int ReceiveTimeout { get; set; }
        void open(Srv s, Options options);
        void close(TcpClient c);
        void makeTLS();
        bool isSetup();
        void teardown();
        Stream getReadBufferedStream();
        Stream getWriteBufferedStream(int size);

    }
}
