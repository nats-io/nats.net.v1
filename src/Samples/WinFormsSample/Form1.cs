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
        private IConnection _subConnection;
        private IConnection _pubConnection;
        private Task _responder;
        private CancellationTokenSource _cts;

        private const string Subject = "queue";

        public Form1()
        {
            InitializeComponent();
        }

        private void ResponderWork()
        {
            using (var s = _subConnection.SubscribeSync(Subject))
            {
                while (!_cts.IsCancellationRequested)
                {
                    var m = s.NextMessage();

                    if (!_cts.IsCancellationRequested)
                    {
                        _subConnection.Publish(m.Reply, m.Data);
                        _subConnection.Flush();
                    }
                }
            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            _cts = new CancellationTokenSource();

            lstScenarios.Items.Add(new Scenario("Request", () =>
            {
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                return Task.Run(() =>
                {
                    var numOfMessages = numMessages.Value;
                    for (var i = 0; i < numOfMessages; i++)
                    {
                        if (!_cts.IsCancellationRequested)
                            _pubConnection.Request(Subject, payload);
                    }
                }, _cts.Token);
            }));

            lstScenarios.Items.Add(new Scenario("RequestAsync", async () =>
            {
                var payload = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

                var numOfMessages = numMessages.Value;
                var configAwait = chkConfigureAwait.Checked;

                for (var i = 0; i < numOfMessages; i++)
                    await _pubConnection.RequestAsync(Subject, payload, _cts.Token).ConfigureAwait(configAwait);
            }));

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
            _subConnection = cnFac.CreateConnection(subOptions);
            _pubConnection = cnFac.CreateConnection(pubOptions);

            _responder = Task.Factory.StartNew(
                ResponderWork,
                _cts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void Form1_FormClosed(object sender, FormClosedEventArgs e)
        {
            _pubConnection?.Dispose();
            _pubConnection = null;

            _cts.Cancel();

            _subConnection?.Dispose();
            _subConnection = null;
        }

        private async void btnRun_Click(object sender, EventArgs e)
        {
            var scenario = lstScenarios.SelectedItem as Scenario;
            if (scenario == null)
                return;

            var configAwait = chkConfigureAwait.Checked;
            var requester = scenario.Action();

            try
            {
                await requester.ConfigureAwait(configAwait);
            }
            catch
            {
                // ignored
            }
        }
    }
}