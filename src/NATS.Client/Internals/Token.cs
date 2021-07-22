using static NATS.Client.Internals.NatsConstants;

namespace NATS.Client.Internals
{
    public enum TokenType {Space, Crlf, Key, Word, Text}
    
    public class Token
    {
        private readonly byte[] _serialized;
        private readonly TokenType _type;
        private readonly int _start;
        private readonly int _end;
        private readonly bool _hasValue;

        internal Token(byte[] serialized, int len, Token prev, TokenType? required) :
            this(serialized, len, prev._end + (prev._type == TokenType.Key ? 2 : 1), required) {}

        internal Token(byte[] serialized, int len, int cur, TokenType? required) 
        {
            _serialized = serialized;

            if (cur >= len) {
                throw new NATSInvalidHeaderException(InvalidHeaderComposition);
            }
            if (serialized[cur] == Sp) {
                _type = TokenType.Space;
                _start = cur;
                _end = cur;
                while (serialized[++cur] == Sp) {
                    _end = cur;
                }
            } else if (serialized[cur] == Cr) {
                MustBeCrlf(len, cur);
                _type = TokenType.Crlf;
                _start = cur;
                _end = cur + 1;
            } else if (required == TokenType.Crlf || required == TokenType.Space) {
                throw new NATSInvalidHeaderException(InvalidHeaderComposition);
            } else {
                byte ender1 = Cr;
                byte ender2 = Cr;
                if (required == null || required == TokenType.Text) {
                    _type = TokenType.Text;
                } else if (required == TokenType.Word) {
                    ender1 = Sp;
                    ender2 = Cr;
                    _type = TokenType.Word;
                } else { // KEY is all that's left if (required == TokenType.KEY) {
                    ender1 = Colon;
                    ender2 = Colon;
                    _type = TokenType.Key;
                }
                _start = cur;
                _end = cur;
                while (++cur < len && serialized[cur] != ender1 && serialized[cur] != ender2) {
                    _end = cur;
                }
                if (cur >= len) {
                    throw new NATSInvalidHeaderException(InvalidHeaderComposition);
                }
                if (serialized[cur] == Cr) {
                    MustBeCrlf(len, cur);
                }
                _hasValue = true;
            }
        }

    private void MustBeCrlf(int len, int cur) {
        if (cur + 1 >= len || _serialized[cur + 1] != Lf) {
            throw new NATSInvalidHeaderException(InvalidHeaderComposition);
        }
    }

    public void MustBe(TokenType expected) {
        if (_type != expected) {
            throw new NATSInvalidHeaderException(InvalidHeaderComposition);
        }
    }

    public bool IsType(TokenType expected) {
        return _type == expected;
    }

    public bool HasValue() {
        return _hasValue;
    }

    public string Value() {
        return _hasValue ? System.Text.Encoding.ASCII.GetString(_serialized, _start, _end - _start + 1).Trim() : Empty;
    }

    public bool SamePoint(Token token) {
        return _start == token._start
                && _end == token._end
                && _type == token._type;
    }
        
    }
}