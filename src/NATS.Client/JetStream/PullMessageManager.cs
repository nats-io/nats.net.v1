using System.Collections.Generic;

namespace NATS.Client.JetStream
{
    internal class PullMessageManager : MessageManager
    {
        private static readonly IList<int> PullKnownStatusCodes = new List<int>(new []{404, 408, 409});

        internal override bool Manage(Msg msg)
        {
            if (!msg.HasStatus) { return false; }
            
            if ( !PullKnownStatusCodes.Contains(msg.Status.Code) ) {
                // pull is always sync
                throw new NATSJetStreamStatusException(Sub, msg.Status);
            }
            return true;
        }
    }
}
