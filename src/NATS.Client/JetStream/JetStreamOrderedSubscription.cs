using System;
using System.Threading.Tasks;

namespace NATS.Client.JetStream
{
    public abstract class AbstractJetStreamOrderedPushSubscription : IJetStreamSubscription
    {
        private JetStream js;
        private string subject;
        private EventHandler<MsgHandlerEventArgs> userHandler;
        private bool isAutoAck;
        private SubscribeOptions so;
        private string stream;
        private ConsumerConfiguration serverCC;

        protected IJetStreamSubscription current;
        protected ulong lastStreamSeq;
        protected ulong expectedConsumerSeq;

        public AbstractJetStreamOrderedPushSubscription(JetStream js, string subject,
            EventHandler<MsgHandlerEventArgs> userHandler, bool isAutoAck, SubscribeOptions so, string stream,
            ConsumerConfiguration serverCc)
        {
            this.js = js;
            this.subject = subject;
            this.userHandler = userHandler;
            this.isAutoAck = isAutoAck;
            this.so = so;
            this.stream = stream;
            serverCC = serverCc;
            lastStreamSeq = 0;
            expectedConsumerSeq = 1; // always starts at 1
        }

        public abstract void SetCurrent(IJetStreamSubscription sub);
        
        protected void SetCurrentInternal(IJetStreamSubscription sub)
        {
            current = sub;
            lastStreamSeq = 0;
            expectedConsumerSeq = 1; // always starts at 1
        }

        public IJetStreamSubscription Current => current;
        
        // IDisposable
        public void Dispose()
        {
            Guarded(() => current.Dispose());
        }

        // ISubscription
        public long Sid => Guarded(() => current.Sid);
        public string Subject => Guarded(() => current.Subject);
        public string Queue => Guarded(() => current.Queue);
        public Connection Connection => current == null ? null : current.Connection;
        public bool IsValid => Guarded(() => current.IsValid);

        public void Unsubscribe()
        {
            Guarded(() => current.Unsubscribe());
        }

        public void AutoUnsubscribe(int max)
        {
            Guarded(() => current.AutoUnsubscribe(max));
        }

        public int QueuedMessageCount => Guarded(() => current.QueuedMessageCount);

        public void SetPendingLimits(long messageLimit, long bytesLimit)
        {
            Guarded(() => current.SetPendingLimits(messageLimit, bytesLimit));
        }

        public long PendingByteLimit
        {
            get { return Guarded(() => current.PendingByteLimit); }
            set { Guarded(() => current.PendingByteLimit = value); }
        }

        public long PendingMessageLimit
        {
            get { return Guarded(() => current.PendingMessageLimit); }
            set { Guarded(() => current.PendingMessageLimit = value); }
        }

        public void GetPending(out long pendingBytes, out long pendingMessages)
        {
            if (current == null)
            {
                pendingBytes = long.MinValue;
                pendingMessages = long.MinValue;
            }
            else
            {
                current.GetPending(out pendingBytes, out pendingMessages);
            }
        }

        public long PendingBytes => Guarded(() => current.PendingBytes);
        public long PendingMessages => Guarded(() => current.PendingMessages);

        public void GetMaxPending(out long maxPendingBytes, out long maxPendingMessages)
        {
            if (current == null)
            {
                maxPendingBytes = long.MinValue;
                maxPendingMessages = long.MinValue;
            }
            else
            {
                current.GetPending(out maxPendingBytes, out maxPendingMessages);
            }
        }

        public long MaxPendingBytes => Guarded(() => current.MaxPendingBytes);
        public long MaxPendingMessages => Guarded(() => current.MaxPendingMessages);
        public void ClearMaxPending() => Guarded(() => current.ClearMaxPending());

        public long Delivered => Guarded(() => current.Delivered);
        public long Dropped => Guarded(() => current.Dropped);

        public void Drain() => Guarded(() => current.Drain());

        public void Drain(int timeout) => Guarded(() => current.Drain(timeout));

        public Task DrainAsync()
        {
            return (Task)Guarded(() => current.DrainAsync());
        }

        public Task DrainAsync(int timeout)
        {
            return (Task)Guarded(() => current.DrainAsync(timeout));
        }

        // IJetStreamSubscription
        public JetStream Context => js;
        public string Stream => stream;
        public string Consumer => Guarded(() => current.Consumer);
        public string DeliverSubject => Guarded(() => current.DeliverSubject);

        public ConsumerInfo GetConsumerInformation()
        {
            return (ConsumerInfo)Guarded(() => current.GetConsumerInformation());
        }

        public bool IsPullMode() => false;

        protected string Guarded(Func<string> supplier)
        {
            return current == null ? null : supplier.Invoke();
        }

        protected int Guarded(Func<int> supplier)
        {
            return current == null ? int.MinValue : supplier.Invoke();
        }

        protected long Guarded(Func<long> supplier)
        {
            return current == null ? long.MinValue : supplier.Invoke();
        }

        protected bool Guarded(Func<bool> supplier) {
            return current != null && supplier.Invoke();
        }

        protected object Guarded(Func<object> supplier) {
            return current == null ? null : supplier.Invoke();
        }

        protected void Guarded(Action action) {
            if (current != null) {
                action.Invoke();
            }
        }

        protected Msg CheckForOutOfOrder(Msg msg)
        {

            if (msg != null)
            {
                ulong receivedConsumerSeq = msg.MetaData.ConsumerSequence;
                if (expectedConsumerSeq != receivedConsumerSeq) {
                    try {
                        current.Unsubscribe();
                    } catch (Exception re) {
                        ((Connection)js.Conn).ScheduleErrEvent((Subscription)current, re.Message);
                    } finally {
                        current = null;
                    }

                    ConsumerConfiguration userCC = ConsumerConfiguration.Builder(serverCC)
                        .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                        .WithDeliverSubject(null)
                        .WithStartSequence(lastStreamSeq + 1)
                        .Build();
                    
                    try 
                    {
                        // Finish Create Subscription calls SetCurrent on the "this" passed in
                        js.FinishCreateSubscription(
                            subject, userHandler, isAutoAck, false, so, stream, null, userCC, null, null, null, this);
                    } 
                    catch (Exception e) 
                    {
                        current = null;
                        
                        ((Connection)js.Conn).ScheduleErrEvent((Subscription)current, e.Message);
                    
                        if (userHandler == null) { // synchronous
                            throw new NATSException("Ordered subscription fatal error.", e);
                        }
                    }
                    
                    // .NET expects a timeout, not a null
                    throw new NATSTimeoutException();
                }

                lastStreamSeq = msg.MetaData.StreamSequence;
                expectedConsumerSeq = receivedConsumerSeq + 1;
            }
            return msg;
        }
    }
    
    public class JetStreamOrderedPushSyncSubscription : AbstractJetStreamOrderedPushSubscription, IJetStreamPushSyncSubscription
    {
        private IJetStreamPushSyncSubscription syncCurrent;
        
        public JetStreamOrderedPushSyncSubscription(JetStream js, string subject, SubscribeOptions so, string stream, ConsumerConfiguration serverCc) 
            : base(js, subject, null, false, so, stream, serverCc)
        {
        }

        public override void SetCurrent(IJetStreamSubscription sub)
        {
            syncCurrent = (IJetStreamPushSyncSubscription)sub;
            SetCurrentInternal(sub);
        }

        // ISyncSubscription
        public Msg NextMessage()
        {
            return (Msg)Guarded(() => CheckForOutOfOrder(syncCurrent.NextMessage()));
        }

        public Msg NextMessage(int timeout)
        {
            return (Msg)Guarded(() => CheckForOutOfOrder(syncCurrent.NextMessage(timeout)));
        }
    }
    
    public class JetStreamOrderedPushAsyncSubscription : AbstractJetStreamOrderedPushSubscription, IJetStreamPushAsyncSubscription, IMessageManager
    {
        private IJetStreamPushAsyncSubscription asyncCurrent;
        
        public JetStreamOrderedPushAsyncSubscription(JetStream js, string subject, EventHandler<MsgHandlerEventArgs> userHandler, bool isAutoAck, SubscribeOptions so, string stream, ConsumerConfiguration serverCc) 
            : base(js, subject, userHandler, isAutoAck, so, stream, serverCc)
        {
        }

        public override void SetCurrent(IJetStreamSubscription sub)
        {
            asyncCurrent = (IJetStreamPushAsyncSubscription)sub;
            SetCurrentInternal(sub);
        }

        public event EventHandler<MsgHandlerEventArgs> MessageHandler;

        public void Start()
        {
            Guarded(() => asyncCurrent.Start());
        }

        bool outOfOrder;
        public bool Manage(Msg msg)
        {
            // if out of order return true which means already managed
            // never pass an out of order message to the user
            if (!outOfOrder)
            {
                outOfOrder = CheckForOutOfOrder(msg) == null;
            }

            return outOfOrder;
        }
    }
}