using static NATS.Client.Internals.NatsConstants;

namespace NATS.Client.Internals
{
    public class HeaderStatusReader
    {
        private int _serializedLength;
        private MsgHeader _header;
        private MsgStatus _status;

        /// <summary>
        /// Parses for MsgHeader and MsgStatus
        /// </summary>
        /// <param name="bytes">A byte array of a serialized MsgStatus/MsgHeader class.</param>
        /// <param name="byteCount">Count of bytes in the serialized array.</param>
        public HeaderStatusReader(byte[] bytes, int byteCount) {

            // basic validation first to help fail fast
            if (bytes == null || bytes.Length == 0) {
                throw new NATSInvalidHeaderException(SerializedHeaderCannotBeNullOrEmpty);
            }
            if (bytes.Length < byteCount)
            {
                throw new NATSInvalidHeaderException("invalid byte count");
            }
            if (byteCount < MinimalValidHeaderLen)
            {
                throw new NATSInvalidHeaderException();
            }

            // is tis the correct version
            for (int x = 0; x < HeaderVersionBytesLen; x++) {
                if (bytes[x] != HeaderVersionBytes[x]) {
                    throw new NATSInvalidHeaderException(InvalidHeaderVersion);
                }
            }

            // does the header end properly
            _serializedLength = byteCount;
            Token terminus = new Token(bytes, _serializedLength, _serializedLength - 2, TokenType.Crlf);
            Token token = new Token(bytes, _serializedLength, HeaderVersionBytesLen, null);

            _header = new MsgHeader();

            bool hadStatus = false;
            if (token.IsType(TokenType.Space)) {
                token = InitStatus(bytes, _serializedLength, token);
                if (token.SamePoint(terminus)) {
                    return; // status only
                }
                hadStatus = true;
            }

            if (token.IsType(TokenType.Crlf)) {
                InitHeader(bytes, _serializedLength, token, hadStatus);
            }
            else {
                throw new NATSInvalidHeaderException(InvalidHeaderComposition);
            }
        }

        public int SerializedLength => _serializedLength;

        public MsgHeader Header => _header;

        public MsgStatus Status => _status;

        private void InitHeader(byte[] serialized, int len, Token tCrlf, bool hadStatus) {
            // REGULAR HEADER
            Token peek = new Token(serialized, len, tCrlf, null);
            while (peek.IsType(TokenType.Text)) {
                Token tKey = new Token(serialized, len, tCrlf, TokenType.Key);
                Token tVal = new Token(serialized, len, tKey, null);
                if (tVal.IsType(TokenType.Space)) {
                    tVal = new Token(serialized, len, tVal, null);
                }
                if (tVal.IsType(TokenType.Text)) {
                    tCrlf = new Token(serialized, len, tVal, TokenType.Crlf);
                }
                else {
                    tVal.MustBe(TokenType.Crlf);
                    tCrlf = tVal;
                }
                _header.Add(tKey.Value(), tVal.Value());
                peek = new Token(serialized, len, tCrlf, null);
            }
            peek.MustBe(TokenType.Crlf);

            if (_header.Count == 0 && !hadStatus) {
                throw new NATSInvalidHeaderException(InvalidHeaderComposition);
            }
        }

        private Token InitStatus(byte[] serialized, int len, Token tSpace) {
            Token tCode = new Token(serialized, len, tSpace, TokenType.Word);
            Token tVal = new Token(serialized, len, tCode, null);
            Token crlf;
            if (tVal.IsType(TokenType.Space)) {
                tVal = new Token(serialized, len, tVal, TokenType.Text);
                crlf = new Token(serialized, len, tVal, TokenType.Crlf);
            }
            else {
                tVal.MustBe(TokenType.Crlf);
                crlf = tVal;
            }
            _status = new MsgStatus(tCode, tVal);
            return crlf;
        }
            
    }
}