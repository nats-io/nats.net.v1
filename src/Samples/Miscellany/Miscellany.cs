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
            args = new[] { "PubSub" };
            
            if (args.Length == 1)
            {
                switch (args[0])
                {
                    case "CrossClientValidation":
                        CrossClientValidation.CrossClientValidationMain();
                        return;
                    
                    case "PubSub":
                        PubSub.PubSubMain();
                        return;
                }
            }
            
            Console.WriteLine(
                "Usage: Miscellany <program>\n"
                + "\nAvailable programs:"
                + "\n   CrossClientValidation" 
                + "\n   PubSub"
            );
        }
    }
}