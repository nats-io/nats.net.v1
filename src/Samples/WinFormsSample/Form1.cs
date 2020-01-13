using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using NATS.Client;

namespace WinFormsSample
{
    public class Scenario
    {
        public string Title { get; }
        public Func<Task> Action { get; }

        public Scenario(string title, Func<Task> action)
        {
            Title = title;
            Action = action;
        }

        public override string ToString() => Title;
    }

    public partial class Form1 : Form
    {
        private IConnection subConnection;
        private IConnection pubConnection;
        private Task responder;
        private CancellationTokenSource cts;

        private const string Subject = "queue";

        public Form1()
        {
            InitializeComponent();
        }

        private void ResponderWork()
        {
            using (var s = subConnection.SubscribeSync(Subject))
            {
                while (!cts.IsCancellationRequested)
                {
                    var m = s.NextMessage();

                    if (!cts.IsCancellationRequested)
                    {
                        subConnection.Publish(m.Reply, m.Data);
                        subConnection.Flush();
                    }
                }
            }
        }

        private int NumOfMessages => Convert.ToInt32(numMessages.Value);

        private void InitializeScenarios()
        {
            if(lstScenarios.Items.Count > 0)
                return;

            lstScenarios.Items.Add(new Scenario("Request", () =>
            {
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                return Task.Run(() =>
                {
                    var numOfMessages = NumOfMessages;
                    for (var i = 0; i < numOfMessages; i++)
                    {
                        if (!cts.IsCancellationRequested)
                            pubConnection.Request(Subject, payload);
                    }
                }, cts.Token);
            }));

            lstScenarios.Items.Add(new Scenario("Request (timeout)", () =>
            {
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                return Task.Run(() =>
                {
                    var numOfMessages = NumOfMessages;
                    for (var i = 0; i < numOfMessages; i++)
                    {
                        if (!cts.IsCancellationRequested)
                            pubConnection.Request(Subject, payload, 150);
                    }
                }, cts.Token);
            }));

            lstScenarios.Items.Add(new Scenario("RequestAsync", async () =>
            {
                var configAwaitFalse = chkConfigureAwaitFalse.Checked;
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                var numOfMessages = NumOfMessages;

                for (var i = 0; i < numOfMessages; i++)
                    await pubConnection.RequestAsync(Subject, payload, cts.Token).ConfigureAwait(!configAwaitFalse);
            }));

            lstScenarios.Items.Add(new Scenario("RequestAsync (timeout)", async () =>
            {
                var configAwaitFalse = chkConfigureAwaitFalse.Checked;
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                var numOfMessages = NumOfMessages;

                for (var i = 0; i < numOfMessages; i++)
                    await pubConnection.RequestAsync(Subject, payload, 150).ConfigureAwait(!configAwaitFalse);
            }));

            lstScenarios.Items.Add(new Scenario("RequestAsync (batched)", async () =>
            {
                var configAwaitFalse = chkConfigureAwaitFalse.Checked;
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                var numOfMessages = NumOfMessages;
                var tasks = new Task<Msg>[numOfMessages];

                for (var i = 0; i < numOfMessages; i++)
                    tasks[i] = pubConnection.RequestAsync(Subject, payload, cts.Token);

                await Task.WhenAll(tasks).ConfigureAwait(!configAwaitFalse);
            }));

            lstScenarios.SelectedItem = lstScenarios.Items[0];
        }

        private void InitializeNats()
        {
            static Options GetOptions()
            {
                var options = ConnectionFactory.GetDefaultOptions();
                options.UseOldRequestStyle = false;
                options.Verbose = false;
                options.NoEcho = true;
                options.Pedantic = false;

                return options;
            }

            var subOptions = GetOptions();
            var pubOptions = GetOptions();

            var cnFac = new ConnectionFactory();
            subConnection = cnFac.CreateConnection(subOptions);
            pubConnection = cnFac.CreateConnection(pubOptions);

            responder = Task.Factory.StartNew(
                ResponderWork,
                cts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            cts = new CancellationTokenSource();

            InitializeScenarios();

            InitializeNats();
        }

        private void Form1_FormClosed(object sender, FormClosedEventArgs e)
        {
            static void Try(Action w)
            {
                try
                {
                    w();
                }
                catch
                {
                    // ignored
                }
            }

            Try(() => pubConnection?.Dispose());
            Try(() => cts?.Cancel());
            Try(() => subConnection?.Dispose());
        }

        private async void btnRun_Click(object sender, EventArgs e)
        {
            btnRun.Enabled = false;

            var scenario = lstScenarios.SelectedItem as Scenario;
            if (scenario == null)
                return;

            try
            {
                await Task.Run(async () =>
                {
                    var configAwaitFalse = chkConfigureAwaitFalse.Checked;
                    var requester = scenario.Action();

                    await requester.ConfigureAwait(!configAwaitFalse);
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Operation failed", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                btnRun.Enabled = true;
            }
        }
    }
}