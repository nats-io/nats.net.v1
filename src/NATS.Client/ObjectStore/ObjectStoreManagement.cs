using System.Collections.Generic;
using System.IO;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using static NATS.Client.ObjectStore.ObjectStoreUtil;

namespace NATS.Client.ObjectStore
{
    public class ObjectStoreManagement : IObjectStoreManagement
    {
        private readonly JetStreamManagement jsm;

        internal ObjectStoreManagement(IConnection connection, ObjectStoreOptions oso)
        {
            jsm = (JetStreamManagement)connection.CreateJetStreamManagementContext(oso?.JSOptions);
        }
        
        public ObjectStoreStatus Create(ObjectStoreConfiguration config)
        {
            StreamConfiguration sc = config.BackingConfig;
            return new ObjectStoreStatus(jsm.AddStream(sc));
        }
        
        public ObjectStoreStatus Update(ObjectStoreConfiguration config)
        {
            return new ObjectStoreStatus(jsm.UpdateStream(config.BackingConfig));
        }

        public IList<string> GetBucketNames()
        {
            IList<string> buckets = new List<string>();
            IList<string> names = jsm.GetStreamNames();
            foreach (string name in names) {
                if (name.StartsWith(ObjStreamPrefix)) {
                    buckets.Add(ExtractBucketName(name));
                }
            }
            return buckets;
        }

        public ObjectStoreStatus GetStatus(string bucketName)
        {
            Validator.ValidateBucketName(bucketName, true);
            return new ObjectStoreStatus(jsm.GetStreamInfo(ToStreamName(bucketName)));
        }
        
        public IList<ObjectStoreStatus> GetStatuses()
        {
            IList<string> bucketNames = GetBucketNames();
            IList<ObjectStoreStatus> statuses = new List<ObjectStoreStatus>();
            foreach (string name in bucketNames) {
                statuses.Add(new ObjectStoreStatus(jsm.GetStreamInfo(ToStreamName(name))));
            }
            return statuses;
        }

        public void Delete(string bucketName)
        {
            Validator.ValidateBucketName(bucketName, true);
            jsm.DeleteStream(ToStreamName(bucketName));
        }
    }
}
