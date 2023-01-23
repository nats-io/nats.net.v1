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

        public virtual string ToJsonString()
        {
            return ToJsonNode().ToString();
        }
    }
}
