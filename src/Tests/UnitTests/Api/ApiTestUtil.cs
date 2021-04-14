using System.IO;
using System.Text;

namespace UnitTests.Api
{
    public class ApiTestUtil
    {
        internal static string ReadDataFile(string name)
        {
            string path = Directory.GetCurrentDirectory();
            string fileSpec = path + "\\..\\..\\..\\Api\\Data\\" + name;
            return File.ReadAllText(fileSpec, Encoding.ASCII);
        }
    }
}