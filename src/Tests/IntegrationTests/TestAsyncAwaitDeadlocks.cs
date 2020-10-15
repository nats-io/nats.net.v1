// Copyright 2015-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#if !NET46
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using NATS.Client;
using Nito.AsyncEx;
using Xunit;

namespace IntegrationTests
{
    public class TestAsyncAwaitDeadlocks : TestSuite<AsyncAwaitDeadlocksSuiteContext>
    {
        public TestAsyncAwaitDeadlocks(AsyncAwaitDeadlocksSuiteContext context) : base(context) { }

        [Fact]
        public void EnsureDrain()
        {
            var subject = "176bb0976ef943e3988de4957c9e1a1a";
            var q = new ConcurrentQueue<int>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        var opts = Context.GetTestOptions(Context.Server1.Port);
                        opts.ClosedEventHandler = (sender, args) =>
                        {
                            q.Enqueue(1);
                            sync.SignalComplete();
                        };

                        using (var cn = Context.ConnectionFactory.CreateConnection(opts))
                        {
                            for (var i = 0; i < 50; i++)
                                cn.Publish(subject, new[] { (byte)i });

                            cn.Drain();

                            sync.WaitForAll();
                            Assert.Single(q);
                        }
                    }
                }
            });
        }

        [Fact]
        public void EnsureDrainAsync()
        {
            var subject = "5dc823d90cab46679e0bf51afffda767";
            var q = new ConcurrentQueue<int>();

            AsyncContext.Run(async () =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        var opts = Context.GetTestOptions(Context.Server1.Port);
                        opts.ClosedEventHandler = (sender, args) =>
                        {
                            q.Enqueue(1);
                            sync.SignalComplete();
                        };

                        using (var cn = Context.ConnectionFactory.CreateConnection(opts))
                        {
                            for (var i = 0; i < 50; i++)
                                cn.Publish(subject, new[] { (byte)i });

                            await cn.DrainAsync();

                            sync.WaitForAll();
                            Assert.Single(q);
                        }
                    }
                }
            });
        }

        [Fact]
        public void EnsureRequestResponder()
        {
            var subject = "751182b093ea42b68d79b69353bf6266";
            var replies = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (cn.SubscribeAsync(subject, (_, m) => m.Message.Respond(m.Message.Data)))
                        {
                            for (var i = 0; i < 5; i++)
                                replies.Enqueue(cn.Request(subject, new[] {(byte) i}));
                        }
                    }
                    
                    Assert.Equal(5, replies.Count);
                }
            });
        }

        [Fact]
        public void EnsureRequestAsyncResponder()
        {
            var subject = "6f7c177f910c4ce587d886cc7212a00f";
            var replies = new ConcurrentQueue<Msg>();

            AsyncContext.Run(async () =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (cn.SubscribeAsync(subject, (_, m) => m.Message.Respond(m.Message.Data)))
                        {
                            for (var i = 0; i < 5; i++)
                                replies.Enqueue(await cn.RequestAsync(subject, new[] {(byte) i}));
                        }
                    }

                    Assert.Equal(5, replies.Count);
                }
            });
        }

        [Fact]
        public void EnsureRequestResponderWithFlush()
        {
            var subject = "67870ca086184482b52815d288a4388e";
            var replies = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (cn.SubscribeAsync(subject, (_, m) => m.Message.Respond(m.Message.Data)))
                        {
                            for (var i = 0; i < 5; i++)
                            {
                                var reply = cn.Request(subject, new[] {(byte) i});
                                cn.Flush();
                                cn.FlushBuffer();
                                replies.Enqueue(reply);
                            }
                        }
                    }
                    
                    Assert.Equal(5, replies.Count);
                }
            });
        }

        [Fact]
        public void EnsureRequestAsyncResponderWithFlush()
        {
            var subject = "3cce135bef9348209dacf9acfc75427e";
            var replies = new ConcurrentQueue<Msg>();

            AsyncContext.Run(async () =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (cn.SubscribeAsync(subject, (_, m) => m.Message.Respond(m.Message.Data)))
                        {
                            for (var i = 0; i < 5; i++)
                            {
                                var reply = await cn.RequestAsync(subject, new[] {(byte) i});
                                cn.Flush();
                                cn.FlushBuffer();
                                replies.Enqueue(reply);
                            }
                        }
                    }

                    Assert.Equal(5, replies.Count);
                }
            });
        }

        [Fact]
        public void EnsurePubSub()
        {
            var subject = "16160bf236cb42dda5bbd0dfc744dacc";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                        {
                            using (cn.SubscribeAsync(subject, (_, m) =>
                            {
                                recieved.Enqueue(m.Message);
                                if(recieved.Count == 5)
                                    sync.SignalComplete();
                            }))
                            {
                                for (var i = 0; i < 5; i++)
                                    cn.Publish(subject, new[] {(byte) i});

                                sync.WaitForAll();
                            }
                        }
                        
                        Assert.Equal(5, recieved.Count);
                    }
                }
            });
        }

        [Fact]
        public void EnsurePubSubWithFlush()
        {
            var subject = "0b122a47380145b49969ddfaafe0c206";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                        {
                            using (cn.SubscribeAsync(subject, (_, m) =>
                            {
                                recieved.Enqueue(m.Message);
                                if(recieved.Count == 5)
                                    sync.SignalComplete();
                            }))
                            {
                                for (var i = 0; i < 5; i++)
                                {
                                    cn.Publish(subject, new[] {(byte) i});
                                    cn.Flush();
                                    cn.FlushBuffer();
                                }

                                sync.WaitForAll();
                            }
                        }
                        
                        Assert.Equal(5, recieved.Count);
                    }
                }
            });
        }

        [Fact]
        public void EnsurePubSubWithAsyncHandler()
        {
            var subject = "236c9af7eae044f585973503e9803f4a";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                        {
                            using (cn.SubscribeAsync(subject, (_, m) =>
                            {
                                recieved.Enqueue(m.Message);
                                if(recieved.Count == 5)
                                    sync.SignalComplete();
                            }))
                            {
                                for (var i = 0; i < 5; i++)
                                    cn.Publish(subject, new[] {(byte) i});

                                sync.WaitForAll();
                            }
                        }
                        
                        Assert.Equal(5, recieved.Count);
                    }
                }
            });
        }
        
        [Fact]
        public void EnsureAutoUnsubscribeForSyncSub()
        {
            var subject = "de82267b22454dd7afc37c9e34a8e0ab";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (var sub = cn.SubscribeSync(subject))
                        {
                            sub.AutoUnsubscribe(1);
                            
                            cn.Publish(subject, new byte[0]);

                            recieved.Enqueue(sub.NextMessage());

                            cn.Publish(subject, new byte[0]);
                            
                            Assert.Equal(1, sub.Delivered);
                        }
                    }
                        
                    Assert.Single(recieved);
                }
            });
        }
        
        [Fact]
        public void EnsureAutoUnsubscribeForAsyncSub()
        {
            var subject = "0be903f6c9c14c10973e78ce03ad47e1";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(async () =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                        {
                            using (var sub = cn.SubscribeAsync(subject, (_, m) =>
                            {
                                recieved.Enqueue(m.Message);
                                sync.SignalComplete();
                            }))
                            {
                                sub.AutoUnsubscribe(1);
                                
                                cn.Publish(subject, new byte[0]);
                                cn.Publish(subject, new byte[0]);
                                
                                sync.WaitForAll();

                                await Task.Delay(100);
                                Assert.Equal(1, sub.Delivered);
                            }
                        }
                        
                        Assert.Single(recieved);
                    }
                }
            });
        }
        
        [Fact]
        public void EnsureUnsubscribeForSyncSub()
        {
            var subject = "b2dcc4f56fd041cb985300d4966bd1c1";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(() =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                    {
                        using (var sub = cn.SubscribeSync(subject))
                        {
                            cn.Publish(subject, new byte[0]);

                            recieved.Enqueue(sub.NextMessage());
                                
                            sub.Unsubscribe();

                            cn.Publish(subject, new byte[0]);
                            Assert.Throws<NATSBadSubscriptionException>(sub.NextMessage);
                            Assert.Equal(1, sub.Delivered);
                        }
                    }
                    
                    Assert.Single(recieved);
                }
            });
        }
        
        [Fact]
        public void EnsureUnsubscribeForAsyncSub()
        {
            var subject = "d37e3729c5c84702b836a4bb4edf7241";
            var recieved = new ConcurrentQueue<Msg>();

            AsyncContext.Run(async () =>
            {
                using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
                {
                    using (var sync = TestSync.SingleActor())
                    {
                        using (var cn = Context.ConnectionFactory.CreateConnection(Context.Server1.Url))
                        {
                            using (var sub = cn.SubscribeAsync(subject, (_, m) =>
                            {
                                recieved.Enqueue(m.Message);
                                sync.SignalComplete();
                            }))
                            {
                                cn.Publish(subject, new byte[0]);
                                sync.WaitForAll();
                                
                                sub.Unsubscribe();
                                
                                cn.Publish(subject, new byte[0]);
                                await Task.Delay(100);
                                Assert.Equal(1, sub.Delivered);
                            }
                        }
                        
                        Assert.Single(recieved);
                    }
                }
            });
        }
    }
}
#endif