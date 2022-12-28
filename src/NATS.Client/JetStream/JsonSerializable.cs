using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public abstract class JsonSerializable
    {
        internal abstract JSONNode ToJsonNode();

        public virtual byte[] Serialize()
        {
            return JsonUtils.Serialize(ToJsonNode());
        }

        public override string ToString()
        {
            return ToJsonNode().ToString();
        }
    }
}
