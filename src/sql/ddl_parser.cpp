#include "sql/ddl_parser.h"

#include <cctype>
#include <optional>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"

namespace kizuna::sql
{
    namespace
    {
        enum class TokenType
        {
            IDENT,
            NUMBER,
            STRING,
            SYMBOL,
            END
        };

        struct Token
        {
            TokenType type{TokenType::END};
            std::string text;
            std::string upper;
            char symbol{0};
            size_t position{0};
        };

        class Lexer
        {
        public:
            explicit Lexer(std::string_view input)
                : input_(input)
            {
                tokenize();
            }

            const std::vector<Token> &tokens() const { return tokens_; }

        private:
            std::string_view input_;
            std::vector<Token> tokens_;

            static bool is_identifier_start(char ch)
            {
                return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
            }

            static bool is_identifier_part(char ch)
            {
                return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
            }

            static std::string to_upper(std::string_view text)
            {
                std::string upper(text);
                for (char &c : upper)
                    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
                return upper;
            }

            void tokenize()
            {
                const char *data = input_.data();
                size_t size = input_.size();
                size_t pos = 0;
                while (pos < size)
                {
                    char ch = data[pos];
                    if (std::isspace(static_cast<unsigned char>(ch)))
                    {
                        ++pos;
                        continue;
                    }

                    if (is_identifier_start(ch))
                    {
                        size_t start = pos;
                        ++pos;
                        while (pos < size && is_identifier_part(data[pos]))
                            ++pos;
                        std::string word(data + start, pos - start);
                        Token tok;
                        tok.type = TokenType::IDENT;
                        tok.text = word;
                        tok.upper = to_upper(word);
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    if (std::isdigit(static_cast<unsigned char>(ch)))
                    {
                        size_t start = pos;
                        ++pos;
                        while (pos < size && std::isdigit(static_cast<unsigned char>(data[pos])))
                            ++pos;
                        std::string number(data + start, pos - start);
                        Token tok;
                        tok.type = TokenType::NUMBER;
                        tok.text = number;
                        tok.upper = number;
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    if (ch == '\'' )
                    {
                        size_t start = pos;
                        ++pos;
                        std::string literal;
                        bool closed = false;
                        while (pos < size)
                        {
                            char curr = data[pos++];
                            if (curr == '\'' )
                            {
                                if (pos < size && data[pos] == '\'' )
                                {
                                    literal.push_back('\'' );
                                    ++pos;
                                }
                                else
                                {
                                    closed = true;
                                    break;
                                }
                            }
                            else
                            {
                                literal.push_back(curr);
                            }
                        }
                        if (!closed)
                        {
                            Token tok;
                            tok.type = TokenType::STRING;
                            tok.text = literal;
                            tok.upper = literal;
                            tok.position = start;
                            tokens_.push_back(std::move(tok));
                            break;
                        }
                        Token tok;
                        tok.type = TokenType::STRING;
                        tok.text = literal;
                        tok.upper = literal;
                        tok.position = start;
                        tokens_.push_back(std::move(tok));
                        continue;
                    }

                    Token tok;
                    tok.type = TokenType::SYMBOL;
                    tok.symbol = ch;
                    tok.text.assign(1, ch);
                    tok.upper = tok.text;
                    tok.position = pos;
                    tokens_.push_back(tok);
                    ++pos;
                }
                tokens_.push_back(Token{TokenType::END, "", "", 0, input_.size()});
            }
        };

        class Parser
        {
        public:
            Parser(std::string_view input, const std::vector<Token> &tokens)
                : input_(input), tokens_(tokens)
            {
            }

            const Token &peek(size_t offset = 0) const
            {
                size_t index = position_ + offset;
                if (index >= tokens_.size())
                    return tokens_.back();
                return tokens_[index];
            }

            const Token &consume()
            {
                return tokens_[position_++];
            }

            bool match_symbol(char symbol)
            {
                if (peek().type == TokenType::SYMBOL && peek().symbol == symbol)
                {
                    ++position_;
                    return true;
                }
                return false;
            }

            void expect_symbol(char symbol)
            {
                if (!match_symbol(symbol))
                {
                    throw QueryException::syntax_error(std::string(input_), peek().position, std::string(1, symbol));
                }
            }

            bool match_keyword(std::string_view keyword)
            {
                if (peek().type == TokenType::IDENT && peek().upper == keyword)
                {
                    ++position_;
                    return true;
                }
                return false;
            }

            void expect_keyword(std::string_view keyword)
            {
                if (!match_keyword(keyword))
                {
                    throw QueryException::syntax_error(std::string(input_), peek().position, std::string(keyword));
                }
            }

            bool match_identifier(Token &out)
            {
                if (peek().type == TokenType::IDENT)
                {
                    out = consume();
                    return true;
                }
                return false;
            }

            Token expect_identifier()
            {
                Token tok;
                if (!match_identifier(tok))
                {
                    throw QueryException::syntax_error(std::string(input_), peek().position, "identifier");
                }
                return tok;
            }

            void expect_end()
            {
                if (peek().type != TokenType::END)
                {
                    if (peek().type == TokenType::SYMBOL && peek().symbol == ';')
                    {
                        ++position_;
                        if (peek().type == TokenType::END)
                            return;
                    }
                    throw QueryException::syntax_error(std::string(input_), peek().position, "end of statement");
                }
            }

            CreateIndexStatement parse_create_index()
            {
                expect_keyword("CREATE");
                CreateIndexStatement stmt;
                if (match_keyword("UNIQUE"))
                {
                    stmt.unique = true;
                }

                expect_keyword("INDEX");

                if (match_keyword("IF"))
                {
                    expect_keyword("NOT");
                    expect_keyword("EXISTS");
                    stmt.if_not_exists = true;
                }

                Token index_name = expect_identifier();
                stmt.index_name = index_name.text;

                expect_keyword("ON");
                Token table_name = expect_identifier();
                stmt.table_name = table_name.text;

                expect_symbol('(');
                bool first = true;
                while (true)
                {
                    if (!first)
                    {
                        if (match_symbol(')'))
                            break;
                        expect_symbol(',');
                    }
                    else
                    {
                        first = false;
                    }

                    Token column_tok = expect_identifier();
                    stmt.column_names.push_back(column_tok.text);

                    if (match_symbol(')'))
                        break;
                }
                expect_end();
                return stmt;
            }

            DropIndexStatement parse_drop_index()
            {
                expect_keyword("DROP");
                expect_keyword("INDEX");

                DropIndexStatement stmt;
                if (match_keyword("IF"))
                {
                    expect_keyword("EXISTS");
                    stmt.if_exists = true;
                }

                Token name = expect_identifier();
                stmt.index_name = name.text;

                expect_end();
                return stmt;
            }
    CreateTableStatement parse_create_table()
            {
                expect_keyword("CREATE");
                expect_keyword("TABLE");
                Token name = expect_identifier();
                expect_symbol('(');

                CreateTableStatement stmt;
                stmt.table_name = name.text;

                bool first_column = true;
                while (true)
                {
                    if (!first_column)
                    {
                        if (match_symbol(')'))
                            break;
                        expect_symbol(',');
                    }
                    else
                    {
                        first_column = false;
                    }

                    ColumnDefAST column = parse_column_def();
                    stmt.columns.push_back(std::move(column));

                    if (match_symbol(')'))
                        break;
                }
                expect_end();
                return stmt;
            }
            DropTableStatement parse_drop_table()
            {
                expect_keyword("DROP");
                expect_keyword("TABLE");

                DropTableStatement stmt;
                if (match_keyword("IF"))
                {
                    expect_keyword("EXISTS");
                    stmt.if_exists = true;
                }

                Token name = expect_identifier();
                stmt.table_name = name.text;

                if (match_keyword("CASCADE"))
                {
                    stmt.cascade = true;
                }

                expect_end();
                return stmt;
            }

            AlterTableStatement parse_alter_table()
            {
                expect_keyword("ALTER");
                expect_keyword("TABLE");

                AlterTableStatement stmt;
                Token table_tok = expect_identifier();
                stmt.table_name = table_tok.text;

                if (match_keyword("ADD"))
                {
                    match_keyword("COLUMN");
                    ColumnDefAST column = parse_column_def();
                    stmt.action = AlterTableAction::ADD_COLUMN;
                    stmt.add_column = std::move(column);
                }
                else if (match_keyword("DROP"))
                {
                    match_keyword("COLUMN");
                    Token column_tok = expect_identifier();
                    stmt.action = AlterTableAction::DROP_COLUMN;
                    stmt.drop_column_name = column_tok.text;
                }
                else
                {
                    throw QueryException::syntax_error(std::string(input_), peek().position, "ADD or DROP");
                }

                expect_end();
                return stmt;
            }

        private:
            std::string_view input_;
            const std::vector<Token> &tokens_;
            size_t position_{0};

            ColumnDefAST parse_column_def()
            {
                Token name_token = expect_identifier();
                ColumnDefAST column;
                column.name = name_token.text;

                Token type_token = expect_identifier();
                parse_data_type(type_token, column);

                parse_column_constraints(column.constraint);
                return column;
            }

            void parse_data_type(const Token &type_token, ColumnDefAST &column)
            {
                const std::string &type_upper = type_token.upper;
                if (type_upper == "INTEGER" || type_upper == "INT")
                {
                    column.type = DataType::INTEGER;
                }
                else if (type_upper == "FLOAT" || type_upper == "DOUBLE")
                {
                    column.type = DataType::FLOAT;
                }
                else if (type_upper == "BOOLEAN" || type_upper == "BOOL")
                {
                    column.type = DataType::BOOLEAN;
                }
                else if (type_upper == "VARCHAR")
                {
                    column.type = DataType::VARCHAR;
                    expect_symbol('(');
                    const Token &len_tok = peek();
                    if (len_tok.type != TokenType::NUMBER)
                        throw QueryException::syntax_error(std::string(input_), len_tok.position, "length");
                    column.length = static_cast<uint32_t>(std::stoul(len_tok.text));
                    consume();
                    expect_symbol(')');
                }
                else if (type_upper == "DATE")
                {
                    column.type = DataType::DATE;
                }
                else
                {
                    throw QueryException::unsupported_type(type_token.text);
                }
            }

            void parse_column_constraints(ColumnConstraintAST &constraint)
            {
                while (true)
                {
                    if (match_keyword("NOT"))
                    {
                        expect_keyword("NULL");
                        constraint.not_null = true;
                        continue;
                    }
                    if (match_keyword("PRIMARY"))
                    {
                        expect_keyword("KEY");
                        constraint.primary_key = true;
                        constraint.not_null = true;
                        constraint.unique = true;
                        continue;
                    }
                    if (match_keyword("UNIQUE"))
                    {
                        constraint.unique = true;
                        continue;
                    }
                    if (match_keyword("DEFAULT"))
                    {
                        const Token &tok = peek();
                        if (tok.type == TokenType::STRING || tok.type == TokenType::NUMBER || tok.type == TokenType::IDENT)
                        {
                            constraint.default_literal = tok.text;
                            consume();
                            continue;
                        }
                        throw QueryException::syntax_error(std::string(input_), tok.position, "default literal");
                    }
                    break;
                }
            }
        };

        std::vector<Token> lex(std::string_view sql)
        {
            Lexer lexer(sql);
            return lexer.tokens();
        }
    } // namespace
    CreateIndexStatement parse_create_index(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        return parser.parse_create_index();
    }

    DropIndexStatement parse_drop_index(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        return parser.parse_drop_index();
    }

    CreateTableStatement parse_create_table(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        return parser.parse_create_table();
    }

    DropTableStatement parse_drop_table(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        return parser.parse_drop_table();
    }

    AlterTableStatement parse_alter_table(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        return parser.parse_alter_table();
    }

    ParsedDDL parse_ddl(std::string_view sql)
    {
        auto tokens = lex(sql);
        Parser parser(sql, tokens);
        ParsedDDL parsed;
        const Token &tok = parser.peek();
        if (tok.type == TokenType::IDENT && tok.upper == "CREATE")
        {
            const Token &next = parser.peek(1);
            if (next.type == TokenType::IDENT && next.upper == "TABLE")
            {
                parsed.kind = StatementKind::CREATE_TABLE;
                parsed.create = parser.parse_create_table();
                return parsed;
            }
            if (next.type == TokenType::IDENT && next.upper == "INDEX")
            {
                parsed.kind = StatementKind::CREATE_INDEX;
                parsed.create_index = parser.parse_create_index();
                return parsed;
            }
            if (next.type == TokenType::IDENT && next.upper == "UNIQUE")
            {
                const Token &after_unique = parser.peek(2);
                if (after_unique.type == TokenType::IDENT && after_unique.upper == "INDEX")
                {
                    parsed.kind = StatementKind::CREATE_INDEX;
                    parsed.create_index = parser.parse_create_index();
                    return parsed;
                }
            }
            parsed.kind = StatementKind::CREATE_TABLE;
            parsed.create = parser.parse_create_table();
            return parsed;
        }
        if (tok.type == TokenType::IDENT && tok.upper == "ALTER")
        {
            parsed.kind = StatementKind::ALTER_TABLE;
            parsed.alter = parser.parse_alter_table();
            return parsed;
        }
        if (tok.type == TokenType::IDENT && tok.upper == "DROP")
        {
            const Token &next = parser.peek(1);
            if (next.type == TokenType::IDENT && next.upper == "TABLE")
            {
                parsed.kind = StatementKind::DROP_TABLE;
                parsed.drop = parser.parse_drop_table();
                return parsed;
            }
            if (next.type == TokenType::IDENT && next.upper == "INDEX")
            {
                parsed.kind = StatementKind::DROP_INDEX;
                parsed.drop_index = parser.parse_drop_index();
                return parsed;
            }
            parsed.kind = StatementKind::DROP_TABLE;
            parsed.drop = parser.parse_drop_table();
            return parsed;
        }
        throw QueryException::syntax_error(std::string(sql), tok.position, "CREATE or DROP");
    }
}
