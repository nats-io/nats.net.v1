using System;
using Xunit.Abstractions;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestSimplification : TestSuite<SimplificationSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestSimplification(ITestOutputHelper output, SimplificationSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }
    }
}