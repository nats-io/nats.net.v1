// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;

namespace NATS.Client
{
    /// <summary>
    /// Provides factory methods to create connections to NATS Servers.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public sealed class ConnectionFactory : IConnectionFactory
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionFactory"/> class,
        /// providing factory methods to create connections to NATS Servers.
        /// </summary>
        public ConnectionFactory() { }

        /// <summary>
        /// Attempt to connect to the NATS server referenced by <paramref name="url"/>.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/> can contain username/password semantics.
        /// Comma seperated arrays are also supported, e.g. <c>&quot;urlA, urlB&quot;</c>.</para>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IConnection CreateConnection(string url, bool reconnectOnConnect = false)
        {
            return CreateConnection(GetDefaultOptions(url), reconnectOnConnect);
        }
        
        /// <summary>
        /// Attempt to connect to the NATS server referenced by <paramref name="url"/> with NATS 2.0 credentials.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/>
        /// Comma seperated arrays are also supported, e.g. <c>&quot;urlA, urlB&quot;</c>.</para>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <param name="credentialsPath">The full path to a chained credentials file.</param>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IConnection CreateConnection(string url, string credentialsPath, bool reconnectOnConnect = false)
        {
            if (string.IsNullOrWhiteSpace(credentialsPath))
                throw new ArgumentException("Invalid credentials path", nameof(credentialsPath));

            Options opts = GetDefaultOptions(url);
            opts.SetUserCredentials(credentialsPath);
            return CreateConnection(opts, reconnectOnConnect);
        }

        /// <summary>
        /// Attempt to connect to the NATS server referenced by <paramref name="url"/> with NATS 2.0 credentials.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/>
        /// Comma seperated arrays are also supported, e.g. <c>&quot;urlA, urlB&quot;</c>.</para>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <param name="jwt">The path to a user's public JWT credentials.</param>
        /// <param name="privateNkey">The path to a file for user user's private Nkey seed.</param>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IConnection CreateConnection(string url, string jwt, string privateNkey, bool reconnectOnConnect = false)
        {
            if (string.IsNullOrWhiteSpace(jwt))
                throw new ArgumentException("Invalid jwt path", nameof(jwt));
            if (string.IsNullOrWhiteSpace(privateNkey))
                throw new ArgumentException("Invalid nkey path", nameof(privateNkey));

            Options opts = GetDefaultOptions(url);
            opts.SetUserCredentials(jwt, privateNkey);
            return CreateConnection(opts, reconnectOnConnect);
        }
        
        /// <summary>
        /// Attempt to connect to the NATS server referenced by <paramref name="url"/>
        /// with NATS 2.0 the user jwt and nkey seed credentials provided directly in the string.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="credentialsText">The text containing the "-----BEGIN NATS USER JWT-----" block
        /// and the text containing the "-----BEGIN USER NKEY SEED-----" block</param>
        /// <param name="reconnectOnConnect"></param>
        /// <returns></returns>
        public IConnection CreateConnectionWithCredentials(string url, string credentialsText, bool reconnectOnConnect = false)
        {
            Options opts = GetDefaultOptions(url);
            opts.SetUserCredentialsFromString(credentialsText);
            return CreateConnection(opts, reconnectOnConnect);
        }

        /// <summary>
        /// Attempt to connect to the NATS server referenced by <paramref name="url"/>
        /// with NATS 2.0 the user jwt and nkey seed credentials provided directly via strings.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/>
        /// Comma seperated arrays are also supported, e.g. <c>&quot;urlA, urlB&quot;</c>.</para>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <param name="userJwtText">The text containing the "-----BEGIN NATS USER JWT-----" block</param>
        /// <param name="nkeySeedText">The text containing the "-----BEGIN USER NKEY SEED-----" block or the seed begining with "SU".
        /// May be the same as the jwt string if they are chained.</param>
        /// <param name="reconnectOnConnect"></param>
        /// <returns></returns>
        public IConnection CreateConnectionWithCredentials(string url, string userJwtText, string nkeySeedText, bool reconnectOnConnect = false)
        {
            Options opts = GetDefaultOptions(url);
            opts.SetUserCredentialsFromStrings(userJwtText, nkeySeedText);
            return CreateConnection(opts, reconnectOnConnect);
        }

        /// <summary>
        /// Retrieves the default set of client options.
        /// </summary>
        /// <param name="server">Optionally set the server. Still can be set or overriden with
        /// <see cref="Options.Url"/> or <see cref="Options.Servers"/> properties</param>
        /// <returns>The default <see cref="Options"/> object for the NATS client.</returns>
        public static Options GetDefaultOptions(string server = null)
        {
            Options opts = new Options();
            if (server != null)
            {
                opts.Url = server;
            }
            return opts;
        }

        /// <summary>
        /// Attempt to connect to the NATS server using TLS referenced by <paramref name="url"/>.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/> can contain username/password semantics.
        /// Comma seperated arrays are also supported, e.g. urlA, urlB.</para>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IConnection CreateSecureConnection(string url, bool reconnectOnConnect = false)
        {
            Options opts = GetDefaultOptions(url);
            opts.Secure = true;
            return CreateConnection(opts, reconnectOnConnect);
        }

        /// <summary>
        /// Create a connection to the NATs server using the default options.
        /// </summary>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        /// <seealso cref="GetDefaultOptions"/>
        public IConnection CreateConnection(bool reconnectOnConnect = false)
        {
            return CreateConnection(GetDefaultOptions(), reconnectOnConnect);
        }

        /// <summary>
        /// Create a connection to a NATS Server defined by the given options.
        /// </summary>
        /// <param name="opts">The NATS client options to use for this connection.</param>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IConnection CreateConnection(Options opts, bool reconnectOnConnect = false)
        {
            Connection nc = new Connection(opts);
            try
            {
                nc.connect(reconnectOnConnect);
            }
            catch (Exception)
            {
                nc.Dispose();
                throw;
            }
            return nc;
        }

        /// <summary>
        /// ENCODED CONNECTIONS, WHILE STILL FUNCTIONAL, WILL NO LONGER BE SUPPORTED
        /// Attempt to connect to the NATS server, with an encoded connection, using the default options.
        /// </summary>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <returns>An <see cref="IEncodedConnection"/> object connected to the NATS server.</returns>
        /// <seealso cref="GetDefaultOptions"/>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IEncodedConnection CreateEncodedConnection(bool reconnectOnConnect = false)
        {
            return CreateEncodedConnection(GetDefaultOptions(), reconnectOnConnect);
        }

        /// <summary>
        /// ENCODED CONNECTIONS, WHILE STILL FUNCTIONAL, WILL NO LONGER BE SUPPORTED
        /// Attempt to connect to the NATS server, with an encoded connection, referenced by <paramref name="url"/>.
        /// </summary>
        /// <remarks>
        /// <para><paramref name="url"/> can contain username/password semantics.
        /// Comma seperated arrays are also supported, e.g. urlA, urlB.</para>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// </remarks>
        /// <param name="url">A string containing the URL (or URLs) to the NATS Server. See the Remarks
        /// section for more information.</param>
        /// <returns>An <see cref="IEncodedConnection"/> object connected to the NATS server.</returns>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IEncodedConnection CreateEncodedConnection(string url, bool reconnectOnConnect = false)
        {
            return CreateEncodedConnection(GetDefaultOptions(url), reconnectOnConnect);
        }

        /// <summary>
        /// ENCODED CONNECTIONS, WHILE STILL FUNCTIONAL, WILL NO LONGER BE SUPPORTED
        /// Attempt to connect to the NATS server, with an encoded connection, using the given options.
        /// </summary>
        /// <param name="opts">The NATS client options to use for this connection.</param>
        /// <returns>An <see cref="IEncodedConnection"/> object connected to the NATS server.</returns>
        /// <param name="reconnectOnConnect">if true, the connection will treat the initial connection as any other and attempt reconnects on failure</param>
        /// <exception cref="NATSNoServersException">No connection to a NATS Server could be established.</exception>
        /// <exception cref="NATSConnectionException"><para>A timeout occurred connecting to a NATS Server.</para>
        /// <para>-or-</para>
        /// <para>An exception was encountered while connecting to a NATS Server. See <see cref="Exception.InnerException"/> for more
        /// details.</para></exception>
        public IEncodedConnection CreateEncodedConnection(Options opts, bool reconnectOnConnect = false)
        {
            EncodedConnection nc = new EncodedConnection(opts);
            try
            {
                nc.connect(reconnectOnConnect);
            }
            catch (Exception)
            {
                nc.Dispose();
                throw;
            }
            return nc;
        }
    }
}
