using System;
using NATSExamples.NatsByExample;

namespace NATSExamples
{
    /// <summary>
    /// This is just scaffolding to be able to run any of the miscellaneous examples
    /// </summary>
    class Miscellany
    {
        static void Main(string[] args)
        {
            // Set the args manually to run the code you want.
            // args = new[] { "ClientCompatibility" };
            
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "PubSub":
                        NatsByExample.PubSub.PubSubMain();
                        return;
                    
                    case "RequestReply":
                        NatsByExample.RequestReply.RequestReplyMain();
                        return;
                                            
                    case "KvIntro":
                        NatsByExample.KvIntro.KvIntroMain();
                        return;
                    
                    case "ScatterGather":
                        ScatterGather.ScatterGatherMain();
                        return;
                    
                    case "ServiceCrossClientValidator":
                        ServiceCrossClientValidator.CrossClientValidationMain();
                        return;
                    
                    case "SimplificationMigration":
                        SimplificationMigration.SimplificationMigrationMain();
                        return;
                    
                    case "ClientCompatibility":
                        ClientCompatibility.ClientCompatibility.ClientCompatibilityMain(args.Length > 1 ? args[1] : null);
                        return;
                }
            }
            
            Console.WriteLine(
                "Usage: Miscellany <program> <args...>\n"
                + "\nAvailable programs:"
                + "\n   PubSub"
                + "\n   RequestReply"
                + "\n   KvIntro"
                + "\n   ScatterGather"
                + "\n   ServiceCrossClientValidator" 
                + "\n   SimplificationMigration" 
                + "\n   ClientCompatibility" 
            );
        }
    }
}