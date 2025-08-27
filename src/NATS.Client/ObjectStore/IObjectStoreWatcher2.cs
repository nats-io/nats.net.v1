namespace NATS.Client.ObjectStore
{
    public interface IObjectStoreWatcher2 : IObjectStoreWatcher
    {
        string getConsumerNamePrefix();
    }
}