namespace NATS.Client.ObjectStore
{
    public interface IObjectStoreWatcherPrefixable : IObjectStoreWatcher
    {
        string getConsumerNamePrefix();
    }
}