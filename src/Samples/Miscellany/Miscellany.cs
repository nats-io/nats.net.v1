using System;

namespace NATSExamples
{
    /// <summary>
    /// This is just scaffolding to be able to run any of the miscellaneous examples
    /// </summary>
    abstract class Miscellany
    {
        public static void Main(string[] args)
        {
            // Set the args manually to run the code you want.
            // args = new[] { "PubSub" };
            
            if (args.Length == 1)
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
                }
            }
            
            Console.WriteLine(
                "Usage: Miscellany <program>\n"
                + "\nAvailable programs:"
                + "\n   PubSub"
                + "\n   RequestReply"
                + "\n   KvIntro"
                + "\n   ScatterGather"
                + "\n   ServiceCrossClientValidator" 
            );
        }
    }
}