// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;
using System.Threading;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;
using System.Xml.Serialization;
using System.IO;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestEncoding
    {
        UnitTestUtilities utils = new UnitTestUtilities();

        [TestInitialize()]
        public void Initialize()
        {
            utils.StartDefaultServer();
        }

        [TestCleanup()]
        public void Cleanup()
        {
            utils.StopDefaultServer();
        }

        [TestMethod]
        public void TestEncodedObjectSerization()
        {
            using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
            {
                String myStr = "value";
                Object mu = new Object();

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    Assert.IsTrue(args.ReceivedObject.Equals(myStr));
                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    lock (mu)
                    {
                        for (int i = 0; i < 10; i++)
                            c.Publish("foo", myStr);

                        c.Flush();

                        Monitor.Wait(mu, 1000);
                    }
                }

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    lock (mu)
                    {
                        c.Publish("foo", "bar", myStr);
                        c.Flush();

                        Monitor.Wait(mu, 1000);
                    }
                }
            }
        }

        [TestMethod]
        public void TestEncodedInvalidObjectSerialization()
        {
            using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
            {
                String myStr = "value";
                Object mu = new Object();

                bool hitException = false;

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    // Ensure we blow up in the cast
                    try
                    {
                        Exception invalid = (Exception)args.ReceivedObject;
                    }
                    catch (Exception e)
                    {
                        hitException = true;
                        System.Console.WriteLine("Expected exception: " + e.Message);
                    }

                    Assert.IsTrue(hitException);

                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    lock (mu)
                    {
                        c.Publish("foo", myStr);
                        c.Flush();

                        Monitor.Wait(mu, 1000);
                    }
                }
            }
        }

        [Serializable]
        public class SerializationTestObj
        {
            public int a = 10;
            public int b = 20;
            public string c = "c";

            public override bool Equals(Object o)
            {
                if (o.GetType() != this.GetType())
                    return false;

                SerializationTestObj to = (SerializationTestObj)o;

                return (a == to.a && b == to.b && c.Equals(to.c));
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("a={0};b={1};c={2}", a, b, c);
                return sb.ToString();
            }
        }

        [TestMethod]
        public void TestCustomObjectSerialization()
        {
            using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
            {
                Object mu = new Object();
                SerializationTestObj origObj = new SerializationTestObj();

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    // Ensure we blow up in the cast
                    SerializationTestObj so = (SerializationTestObj)args.ReceivedObject;
                    Assert.IsTrue(so.Equals(origObj));

                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    lock (mu)
                    {
                        c.Publish("foo", new SerializationTestObj());
                        c.Flush();

                        Monitor.Wait(mu, 1000);
                    }
                }
            }
        }

        // IF this works, all XML related subclassing should work 
        // (e.g. JSON, DataContractFormatters, etc, as well as any other
        // custom formatting.
        byte[] serializeToXML(Object obj)
        {
            MemoryStream  ms = new MemoryStream();
            XmlSerializer x = new XmlSerializer(((SerializationTestObj)obj).GetType());

            x.Serialize(ms, obj);

            byte[] content = new byte[ms.Position];
            Array.Copy(ms.GetBuffer(), content, ms.Position);

            System.Console.WriteLine("Content: " + Encoding.ASCII.GetString(content));

            return content;
        }

        Object deserializeFromXML(byte[] data)
        {
            XmlSerializer x = new XmlSerializer(new SerializationTestObj().GetType());
            MemoryStream ms = new MemoryStream(data);
            return x.Deserialize(ms);
        }

        [TestMethod]
        public void TestEncodedSerizationOverrides()
        {
            using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
            {
                c.OnDeserialize = deserializeFromXML;
                c.OnSerialize = serializeToXML;

                Object mu = new Object();
                SerializationTestObj origObj = new SerializationTestObj();
                origObj.a = 99;

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    SerializationTestObj so = (SerializationTestObj)args.ReceivedObject;
                    Assert.IsTrue(so.Equals(origObj));

                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    lock (mu)
                    {
                        c.Publish("foo", origObj);
                        c.Flush();

                        Monitor.Wait(mu, 1000);
                    }
                }
            }
        }

        [TestMethod]
        public void TestEncodedObjectRequestReply()
        {
            using (IEncodedConnection c = new ConnectionFactory().CreateEncodedConnection())
            {
                Object mu = new Object();
                SerializationTestObj origObj = new SerializationTestObj();

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    SerializationTestObj so = (SerializationTestObj)args.ReceivedObject;
                    Assert.IsTrue(so.Equals(origObj));
                    String str = "Received";

                    c.Publish(args.Reply, str);
                    c.Flush();

                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                {
                    Assert.IsTrue("Received".Equals(c.Request("foo", origObj, 1000)));
                    Assert.IsTrue("Received".Equals(c.Request("foo", origObj)));
                }
            }
        }

    } // class

} // namespace
