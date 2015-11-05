// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Text;

// disable XML comment warnings
#pragma warning disable 1591

namespace NATS.Client
{
    public class Subscription : ISubscription, IDisposable
    {
        readonly  internal  Object mu = new Object(); // lock

        internal  long           sid = 0; // subscriber ID.
        private   long           msgs;
        internal  protected long delivered;
        private   long           bytes;
        internal  protected long max = -1;

        // slow consumer
        internal bool       sc   = false;

        internal Connection conn = null;
        internal Channel<Msg> mch  = new Channel<Msg>();

        // Subject that represents this subscription. This can be different
        // than the received subject inside a Msg if this is a wildcard.
        private string      subject = null;

        internal Subscription(Connection conn, string subject, string queue)
        {
            this.conn = conn;
            this.subject = subject;
            this.queue = queue;
        }

        internal void closeChannel()
        {
            lock (mu)
            {
                mch.close();
                mch = null;
            }
        }
 
        public string Subject
        {
            get { return subject; }
        }

        // Optional queue group name. If present, all subscriptions with the
        // same name will form a distributed queue, and each message will
        // only be processed by one member of the group.
        string queue;

        public string Queue
        {
            get { return queue; }
        }

        public Connection Connection
        {
            get
            {
                return conn;
            }
        }
 
        internal bool tallyMessage(long bytes)
        {
            lock (mu)
            {
                if (max > 0 && msgs > max)
                    return true;

                this.msgs++;
                this.bytes += bytes;

            }

            return false;
        }


        protected internal virtual bool processMsg(Msg msg)
        {
            return true;
        }

        // returns false if the message could not be added because
        // the channel is full, true if the message was added
        // to the channel.
        internal bool addMessage(Msg msg, int maxCount)
        {
            if (mch != null)
            {
                if (mch.Count >= maxCount)
                {
                    return false;
                }
                else
                {
                    sc = false;
                    mch.add(msg);
                }
            }
            return true;
        }

        public bool IsValid
        {
            get
            {
                lock (mu)
                {
                    return (conn != null);
                }
            }
        }

        public virtual void Unsubscribe()
        {
            Connection c;
            lock (mu)
            {
                c = this.conn;
            }

            if (c == null)
                throw new NATSBadSubscriptionException();

            c.unsubscribe(this, 0);
        }

        public virtual void AutoUnsubscribe(int max)
        {
            Connection c = null;

            lock (mu)
            {
                if (conn == null)
                    throw new NATSBadSubscriptionException();

                c = conn;
            }

            c.unsubscribe(this, max);
        }

        public int QueuedMessageCount
        {
            get
            {
                lock (mu)
                {
                    if (this.conn == null)
                        throw new NATSBadSubscriptionException();

                    return mch.Count;
                }
            }
        }

        void IDisposable.Dispose()
        {
            try
            {
                Unsubscribe();
            }
            catch (Exception)
            { 
                // We we get here with normal usage, for example when
                // auto unsubscribing, so just this here.
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("{");
            
            sb.AppendFormat("Subject={0};Queue={1};" +
                "QueuedMessageCount={2};IsValid={3};Type={4}",
                Subject, (Queue == null ? "null" : Queue), 
                QueuedMessageCount, IsValid, 
                this.GetType().ToString());
            
            sb.Append("}");
            
            return sb.ToString();
        }

    }  // Subscription

}