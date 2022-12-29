using System;

namespace NATSExamples
{
    /// <summary>
    /// This is just scaffolding to be able to run any of the miscellaneous examples
    /// </summary>
    class Miscellany
    {
        public static void Main(string[] args)
        {
            // Set the args manually to run the code you want.
            // args = new[] { "RequestReply" };
            
            if (args.Length == 1)
            {
                switch (args[0])
                {
                    case "CrossClientValidation":
                        CrossClientValidation.CrossClientValidationMain();
                        return;
                    
                    case "PubSub":
                        NatsByExample.PubSub.PubSubMain();
                        return;
                    
                    case "RequestReply":
                        NatsByExample.RequestReply.RequestReplyMain();
                        return;
                                            
                    case "KvIntro":
                        NatsByExample.KvIntro.KvIntroMain();
                        return;
                }
            }
            
            Console.WriteLine(
                "Usage: Miscellany <program>\n"
                + "\nAvailable programs:"
                + "\n   CrossClientValidation" 
                + "\n   PubSub"
                + "\n   RequestReply"
                + "\n   KvIntro"
            );
        }
    }
}