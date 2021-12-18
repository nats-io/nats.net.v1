using System;
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;

namespace NATSExamples
{
    /// <summary>
    /// This example will demonstrate many KeyValue features.
    /// </summary>
    internal static class KeyValueFull
    {
        private const string Usage = 
            "Usage: KeyValueFull [-url url] [-creds file] [-stream stream] " +
            "[-subject subject] [-count count] [-payload payload] [-header key:value]" +
            "\n\nDefault Values:" +
            "\n   [-stream]   example-stream" +
            "\n   [-subject]  example-subject" +
            "\n   [-payload]  Hello" +
            "\n   [-count]    10" +
            "\n\nRun Notes:" +
            "\n   - count < 1 is the same as 1" +
            "\n   - quote multi word payload" +
            "\n   - headers are optional, quote multi word value, no colons ':' in value please!";

        private const string ByteKey = "byteKey";
        private const string StringKey = "stringKey";
        private const string LongKey = "longKey";
        private const string NotFound = "notFound";

        public static void Main(string[] args)
        {
            ArgumentHelper helper = new ArgumentHelperBuilder("KeyValueFull Full", args, Usage)
                .DefaultBucket("exampleBucket")
                .DefaultDescription("Example Description")
                .Build();

            try
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(helper.MakeOptions()))
                {
                    // get the kv management context
                    IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                    // create the bucket
                    KeyValueConfiguration bc = KeyValueConfiguration.Builder()
                        .WithName(helper.Bucket)
                        .WithDescription(helper.Description)
                        .WithMaxHistoryPerKey(5)
                        .WithStorageType(StorageType.Memory)
                        .Build();

                    KeyValueStatus kvs = kvm.Create(bc);
                    Console.WriteLine(kvs);

                    // get the kv context for the specific bucket
                    IKeyValue kv = c.CreateKeyValueContext(helper.Bucket);

                    // Put some keys. Each key is put in a subject in the bucket (stream)
                    // The put returns the revision number in the bucket (stream)
                    Console.WriteLine("\n1. Put");

                    ulong seq = kv.Put(ByteKey, Encoding.UTF8.GetBytes("Byte Value 1"));
                    Console.WriteLine("Revision number should be 1, got " + seq);

                    seq = kv.Put(StringKey, "String Value 1");
                    Console.WriteLine("Revision number should be 2, got " + seq);

                    seq = kv.Put(LongKey, 1);
                    Console.WriteLine("Revision number should be 3, got " + seq);

                    // retrieve the values. all types are stored as bytes
                    // so you can always get the bytes directly
                    Console.WriteLine("\n2. Get Value (Bytes)");

                    byte[] bvalue = kv.Get(ByteKey).Value;
                    Console.WriteLine(ByteKey + " from Value property: " + Encoding.UTF8.GetString(bvalue));

                    bvalue = kv.Get(StringKey).Value;
                    Console.WriteLine(StringKey + " from Value property: " + Encoding.UTF8.GetString(bvalue));

                    bvalue = kv.Get(LongKey).Value;
                    Console.WriteLine(LongKey + " from Value property: " + Encoding.UTF8.GetString(bvalue));

                    // if you know the value is not binary and can safely be read
                    // as a UTF-8 string, the ValueAsString function is ok to use
                    Console.WriteLine("\n3. Get String Value");

                    String svalue = kv.Get(ByteKey).ValueAsString();
                    Console.WriteLine(ByteKey + " from ValueAsString(): " + svalue);

                    svalue = kv.Get(StringKey).ValueAsString();
                    Console.WriteLine(StringKey + " from ValueAsString(): " + svalue);

                    svalue = kv.Get(LongKey).ValueAsString();
                    Console.WriteLine(LongKey + " from ValueAsString(): " + svalue);

                    // if you know the value is a long, you can use
                    // the getLongValue method
                    // if it's not a number a NumberFormatException is thrown
                    Console.WriteLine("\n4. Get Long Value");

                    long lvalue;
                    bool bLongGet = kv.Get(LongKey).TryGetLongValue(out lvalue);
                    Console.WriteLine(LongKey + " from getValueAsLong: " + lvalue);

                    bLongGet = kv.Get(StringKey).TryGetLongValue(out lvalue);
                    if (!bLongGet)
                    {
                        Console.WriteLine(StringKey + " value is not a long!");
                    }

                    // entry gives detail about latest record of the key
                    Console.WriteLine("\n5. Get Entry");

                    KeyValueEntry entry = kv.Get(ByteKey);
                    Console.WriteLine(ByteKey + " entry: " + entry);

                    entry = kv.Get(StringKey);
                    Console.WriteLine(StringKey + " entry: " + entry);

                    entry = kv.Get(LongKey);
                    Console.WriteLine(LongKey + " entry: " + entry);

                    // delete a key
                    Console.WriteLine("\n6. Delete a key");
                    kv.Delete(ByteKey);

                    // it's value is now null
                    // it's value is now null but there is a delete tombstone
                    KeyValueEntry kve = kv.Get(ByteKey);
                    Console.WriteLine("Delete tombstone entry: " + kve);
                    Console.WriteLine("Revision number should be 4, got " + kve.Revision);
                    Console.WriteLine("Deleted value should be null: " + (kve.Value == null));

                    // if the key does not exist there is no entry at all
                    Console.WriteLine("\n7.1 Keys does not exist");
                    kve = kv.Get(NotFound);
                    Console.WriteLine($"Entry for {NotFound} should be null: {kve == null}");

                    // if the key has been deleted there is an entry for it
                    // but the value will be null
                    Console.WriteLine("\n7.2 Keys not found");
                    bvalue = kv.Get(ByteKey).Value;
                    Console.WriteLine($"{ByteKey} byte value should be null: {bvalue == null}");

                    svalue = kv.Get(ByteKey).ValueAsString();
                    Console.WriteLine($"{ByteKey} string value should be null: " + (svalue == null));

                    bLongGet = kv.Get(ByteKey).TryGetLongValue(out lvalue);
                    if (!bLongGet)
                    {
                        Console.WriteLine(ByteKey + " value is not a long!");
                    }

                    // Update values. You can even update a deleted key
                    Console.WriteLine("\n8.1 Update values");
                    seq = kv.Put(ByteKey, Encoding.UTF8.GetBytes("Byte Value 2"));
                    Console.WriteLine("Revision number should be 5, got " + seq);

                    seq = kv.Put(StringKey, "String Value 2");
                    Console.WriteLine("Revision number should be 6, got " + seq);

                    seq = kv.Put(LongKey, 2);
                    Console.WriteLine("Revision number should be 7, got " + seq);

                    // values after updates
                    Console.WriteLine("\n8.2 Values after update");

                    svalue = kv.Get(ByteKey).ValueAsString();
                    Console.WriteLine(ByteKey + " from ValueAsString(): " + svalue);

                    svalue = kv.Get(StringKey).ValueAsString();
                    Console.WriteLine(StringKey + " from ValueAsString(): " + svalue);

                    bLongGet = kv.Get(LongKey).TryGetLongValue(out lvalue);
                    Console.WriteLine(LongKey + " from TryGetLongValue: " + lvalue);

                    // let's check the bucket info
                    Console.WriteLine("\n9.1 Bucket before delete");
                    kvs = kvm.GetBucketInfo(helper.Bucket);
                    Console.WriteLine(kvs);

                    // delete the bucket
                    Console.WriteLine("\n9.2 Delete");
                    kvm.Delete(helper.Bucket);

                    try {
                        kvm.GetBucketInfo(helper.Bucket);
                        Console.WriteLine("UH OH! Bucket should not have been found!");
                    }
                    catch (NATSJetStreamException) {
                        Console.WriteLine("Bucket was not found!");
                    }
                }
            }
            catch (Exception ex)
            {
                helper.ReportException(ex);
            }
        }
    }
}
