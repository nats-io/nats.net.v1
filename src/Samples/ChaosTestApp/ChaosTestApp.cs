using System;
using System.Collections.Generic;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;

namespace NATSExamples
{
    public static class App
    {
        const string AppLabel = "APP";

        public static String[] ManualArgs = (
            // "--servers nats://192.168.50.99:4222"
            "--servers nats://localhost:4222"
            + " --stream chaos-stream"
            + " --subject chaos-subject"
//            + " --runtime 3600 // 1 hour in seconds
            + " --create"
            // + " --r3"
            + " --publish"
            + " --pubjitter 30"
            + " --simple ordered 100 5000"
            + " --simple durable 100 5000" // space or commas work, the parser figures it out
            + " --fetch durable,100,5000"
            + " --push ordered"
            + " --push durable"
        ).Split(' ');

        static void Main(string[] args)
        {
            args = ManualArgs; // comment out for real command line
            
            CommandLine cmd = new CommandLine(args);
            Monitor monitor;
            try
            {
                Output.Start(cmd);
                Output.ControlMessage(AppLabel, cmd.ToString().Replace(" --", "    \n--"));
                CountdownEvent waiter = new CountdownEvent(1);

                ChaosPublisher publisher = null;
                IList<ConnectableConsumer> cons = null;

                if (cmd.create) {
                    Options opts = ConnectionFactory.GetDefaultOptions();
                    opts.Url = cmd.Servers;
                    opts.MaxReconnect = -1;
                    using (IConnection conn = new ConnectionFactory().CreateConnection(opts))
                    {
                        Console.WriteLine(conn.ServerInfo);
                        IJetStreamManagement jsm = conn.CreateJetStreamManagementContext();
                        CreateOrReplaceStream(cmd, jsm);
                    }
                }

                if (cmd.CommandLineConsumers.Count > 0)
                {
                    cons = new List<ConnectableConsumer>();
                    foreach (CommandLineConsumer clc in cmd.CommandLineConsumers) {
                        ConnectableConsumer con;
                        switch (clc.consumerType) {
                            case ConsumerType.Push:
                                con = new PushConsumer(cmd, clc.consumerKind);
                                break;
                            case ConsumerType.Simple:
                                con = new SimpleConsumer(cmd, clc.consumerKind, clc.batchSize, clc.expiresIn);
                                break;
                            case ConsumerType.Fetch:
                                con = new SimpleFetchConsumer(cmd, clc.consumerKind, clc.batchSize, clc.expiresIn);
                                break;
                            default:
                                throw new ArgumentException("Unsupported consumer type: " + clc.consumerType);
                        }
                        Output.ControlMessage(AppLabel, con.Label);
                        cons.Add(con);
                    }
                }

                if (cmd.Publish) {
                    publisher = new ChaosPublisher(cmd, cmd.PubJitter);
                    Thread pubThread = new Thread(publisher.Run);
                    pubThread.Start();
                }

                // just creating the stream?
                if (publisher == null && cons == null) {
                    return;
                }

                monitor = new Monitor(cmd, publisher, cons);
                Thread monThread = new Thread(monitor.Run);
                monThread.Start();

                int runtime = cmd.Runtime < 1 ? int.MaxValue : cmd.Runtime;
                //noinspection ResultOfMethodCallIgnored
                waiter.Wait(runtime);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
         
        public static void CreateOrReplaceStream(CommandLine cmd, IJetStreamManagement jsm) {
            try {
                jsm.DeleteStream(cmd.Stream);
            }
            catch (Exception ignore) {}
            try {
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(cmd.Stream)
                    .WithStorageType(StorageType.File)
                    .WithSubjects(cmd.Subject)
                    .WithReplicas(cmd.R3 ? 3 : 1)
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                Output.ControlMessage(AppLabel, "Create Stream\n" + si.Config.ToJsonNode().ToString());
            }
            catch (Exception e) {
                Output.FatalMessage(AppLabel, "Failed creating stream: '" + cmd.Stream + "' " + e);
                Environment.Exit(-1);
            }
        }
    }
}