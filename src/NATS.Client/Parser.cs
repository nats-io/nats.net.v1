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
using System.IO;
     
namespace NATS.Client
{
    internal sealed class MsgArg
    {
	    internal string subject;
	    internal string reply;
	    internal long   sid;
        internal int    hdr;
	    internal int    size;
    }

    internal sealed class Parser
    {
        readonly Connection conn;
        readonly byte[] argBufBase = new byte[Defaults.defaultBufSize];
        readonly MemoryStream argBufStream = null;

        byte[] msgBufBase = new byte[Defaults.defaultBufSize];
        MemoryStream msgBufStream = null;

        internal Parser(Connection conn)
        {
            argBufStream = new MemoryStream(argBufBase);
            msgBufStream = new MemoryStream(msgBufBase);

            this.conn = conn;
        }

        internal int state = 0;
        internal int hdr = 0;

        // For performance declare these as consts - they'll be
        // baked into the IL code (thus faster).  An enum would
        // be nice, but we want speed in this critical section of
        // message handling.
        private const int OP_START         = 0;
        private const int OP_PLUS          = 1;
        private const int OP_PLUS_O        = 2;
        private const int OP_PLUS_OK       = 3;
        private const int OP_MINUS         = 4;
        private const int OP_MINUS_E       = 5;
        private const int OP_MINUS_ER      = 6;
        private const int OP_MINUS_ERR     = 7;
        private const int OP_MINUS_ERR_SPC = 8;
        private const int MINUS_ERR_ARG    = 9;
        private const int OP_C             = 10;
        private const int OP_CO            = 11;
        private const int OP_CON           = 12;
        private const int OP_CONN          = 13;
        private const int OP_CONNE         = 14;
        private const int OP_CONNEC        = 15;
        private const int OP_CONNECT       = 16;
        private const int CONNECT_ARG      = 17;
        private const int OP_M             = 18;
        private const int OP_MS            = 19;
        private const int OP_MSG           = 20;
        private const int OP_MSG_SPC       = 21;
        private const int MSG_ARG          = 22;
        private const int MSG_PAYLOAD      = 23;
        private const int MSG_END          = 24;
        private const int OP_P             = 25;
        private const int OP_H             = 26;
        private const int OP_PI            = 27;
        private const int OP_PIN           = 28;
        private const int OP_PING          = 29;
        private const int OP_PO            = 30;
        private const int OP_PON           = 31;
        private const int OP_PONG          = 32;
        private const int OP_I             = 33;
        private const int OP_IN            = 34;
        private const int OP_INF           = 35;
        private const int OP_INFO          = 36;
        private const int OP_INFO_SPC      = 37;
        private const int INFO_ARG         = 38;

        private void parseError(byte[] buffer, int position)
        {
            throw new NATSException(string.Format("Parse Error [{0}], {1}", state, buffer));
        }

        internal void parse(byte[] buffer, int len)
        {
            int i;
            char b;

            for (i = 0; i < len; i++)
            {
                b = (char)buffer[i];

                switch (state)
                {
                    case OP_START:
                        switch (b)
                        {
                            case 'M':
                            case 'm':
                                state = OP_M;
                                hdr = -1;
                                break;
                            case 'H':
                            case 'h':
                                state = OP_H;
                                hdr = 1;
                                break;
                            case 'C':
                            case 'c':
                                state = OP_C;
                                break;
                            case 'P':
                            case 'p':
                                state = OP_P;
                                break;
                            case '+':
                                state = OP_PLUS;
                                break;
                            case '-':
                                state = OP_MINUS;
                                break;
                            case 'i':
                            case 'I':
                                state = OP_I;
                                break;
                            default:
                                parseError(buffer,i);
                                break;
                        }
                        break;
                    case OP_H:
                        switch(b)
                        {
                            case 'M':
                            case 'm':
                                state = OP_M;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_M:
                        switch (b)
                        {
                            case 'S':
                            case 's':
                                state = OP_MS;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MS:
                        switch (b)
                        {
                            case 'G':
                            case 'g':
                                state = OP_MSG;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MSG:
                        switch (b)
                        {
                            case ' ':
                            case '\t':
                                state = OP_MSG_SPC;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MSG_SPC:
                        switch (b)
                        {
                            case ' ':
                                break;
                            case '\t':
                                break;
                            default:
                                state = MSG_ARG;
                                i--;
                                break;
                        }
                        break;
                    case MSG_ARG:
                        switch (b)
                        {
                            case '\r':
                                break;
                            case '\n':
                                if (hdr >= 0)
                                {
                                    conn.processHeaderMsgArgs(argBufBase, argBufStream.Position);
                                }
                                else
                                {
                                    conn.processMsgArgs(argBufBase, argBufStream.Position);
                                }
                                argBufStream.Position = 0;
                                if (conn.msgArgs.size > msgBufBase.Length)
                                {
                                    // Add 2 to account for the \r\n
                                    msgBufBase = new byte[conn.msgArgs.size+2];

                                    msgBufStream?.Dispose();
                                    msgBufStream = new MemoryStream(msgBufBase);
                                }
                                state = MSG_PAYLOAD;
                                break;
                            default:
                                argBufStream.WriteByte((byte)b);
                                break;
                        }
                        break;
                    case MSG_PAYLOAD:
                        int  msgSize  = conn.msgArgs.size;
                        if (msgSize == 0)
                        {
                            conn.processMsg(msgBufBase, msgSize);
                            state = MSG_END;
                        }
                        else
                        {
                            long position = msgBufStream.Position;
                            int writeLen = msgSize - (int)position;
                            int avail = len - i;

                            if (avail < writeLen)
                            {
                                writeLen = avail;
                            }

                            msgBufStream.Write(buffer, i, writeLen);
                            i += (writeLen - 1);

                            if ((position + writeLen) >= msgSize)
                            {
                                conn.processMsg(msgBufBase, msgSize);
                                msgBufStream.Position = 0;
                                state = MSG_END;
                            }
                        }
                        break;
                    case MSG_END:
                        switch (b)
                        {
                            case '\n':
                                state = OP_START;
                                break;
                            default:
                                continue;
                        }
                        break;
                    case OP_PLUS:
                        switch (b)
                        {
                            case 'O':
                            case 'o':
                                state = OP_PLUS_O;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PLUS_O:
                        switch (b)
                        {
                            case 'K':
                            case 'k':
                                state = OP_PLUS_OK;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PLUS_OK:
                        switch (b)
                        {
                            case '\n':
                                conn.processOK();
                                state = OP_START;
                                break;
                        }
                        break;
                    case OP_MINUS:
                        switch (b)
                        {
                            case 'E':
                            case 'e':
                                state = OP_MINUS_E;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MINUS_E:
                        switch (b)
                        {
                            case 'R':
                            case 'r':
                                state = OP_MINUS_ER;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MINUS_ER:
                        switch (b)
                        {
                            case 'R':
                            case 'r':
                                state = OP_MINUS_ERR;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MINUS_ERR:
                        switch (b)
                        {
                            case ' ':
                            case '\t':
                                state = OP_MINUS_ERR_SPC;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_MINUS_ERR_SPC:
                        switch (b)
                        {
                            case ' ':
                            case '\t':
                                state = OP_MINUS_ERR_SPC;
                                break;
                            default:
                                state = MINUS_ERR_ARG;
                                i--;
                                break;
                        }
                        break;
                    case MINUS_ERR_ARG:
                        switch (b)
                        {
                            case '\r':
                                break;
                            case '\n':
                                conn.processErr(argBufStream);
                                argBufStream.Position = 0;
                                state = OP_START;
                                break;
                            default:
                                argBufStream.WriteByte((byte)b);
                                break;
                        }
                        break;
                    case OP_P:
                        switch (b)
                        {
                            case 'I':
                            case 'i':
                                state = OP_PI;
                                break;
                            case 'O':
                            case 'o':
                                state = OP_PO;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PO:
                        switch (b)
                        {
                            case 'N':
                            case 'n':
                                state = OP_PON;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PON:
                        switch (b)
                        {
                            case 'G':
                            case 'g':
                                state = OP_PONG;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PONG:
                        switch (b)
                        {
                            case '\r':
                                break;
                            case '\n':
                                conn.processPong();
                                state = OP_START;
                                break;
                        }
                        break;
                    case OP_PI:
                        switch (b)
                        {
                            case 'N':
                            case 'n':
                                state = OP_PIN;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PIN:
                        switch (b)
                        {
                            case 'G':
                            case 'g':
                                state = OP_PING;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_PING:
                        switch (b)
                        {
                            case '\r':
                                break;
                            case '\n':
                                conn.processPing();
                                state = OP_START;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_I:
                        switch (b)
                        {
                            case 'N':
                            case 'n':
                                state = OP_IN;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_IN:
                        switch (b)
                        {
                            case 'F':
                            case 'f':
                                state = OP_INF;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_INF:
                        switch (b)
                        {
                            case 'O':
                            case 'o':
                                state = OP_INFO;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_INFO:
                        switch (b)
                        {
                            case ' ':
                            case '\t':
                                state = OP_INFO_SPC;
                                break;
                            default:
                                parseError(buffer, i);
                                break;
                        }
                        break;
                    case OP_INFO_SPC:
                        switch (b)
                        {
                            case ' ':
                            case '\t':
                                break;
                            default:
                                argBufStream.Position = 0;
                                state = INFO_ARG;
                                i--;
                                break;
                        }
                        break;
                    case INFO_ARG:
                        switch (b)
                        {
                            case '\r':
                                break;
                            case '\n':
                                conn.processAsyncInfo(argBufBase, (int)argBufStream.Position);
								argBufStream.Position = 0;
                                state = OP_START;
                                break;
                            default:
                                argBufStream.WriteByte((byte)b);
                                break;
                        }
                        break;
                    default:
                        throw new NATSException("Unable to parse.");
                } // switch(state)

            }  // for
     
        } // parse

    }
}