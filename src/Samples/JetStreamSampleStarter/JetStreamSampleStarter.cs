using System;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    /// <summary>
    /// This is just a class to get you started on your own...
    /// </summary>
    public static class JetStreamSampleStarter
    {
        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("JetStreamSampleStarter", args, "")
                .DefaultStream("starter-stream")
                .DefaultSubject("starter-subject")
                .DefaultPayload("Hello World")
                .DefaultCount(10)
                .Build();
            
            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // create a JetStream context
                    IJetStream js = c.CreateJetStreamContext();
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
           
        }
    }
}