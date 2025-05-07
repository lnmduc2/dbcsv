from lark import Lark, Transformer, Token

grammar = r"""
    start: select_statement

    select_statement: kw_select column_list kw_from table_name [where_clause]

    column_list: ASTERISK | column_name ("," column_name)*

    column_name: CNAME
    table_name: CNAME

    ASTERISK: "*"

    where_clause: kw_where condition

    condition: expression

    expression: comparison_expression
              | LPAREN expression RPAREN -> paren_expr
              | expression kw_and expression -> and_expr
              | expression kw_or expression  -> or_expr

    comparison_expression: operand COMPARISON_OP operand
                         | operand kw_like operand      -> like_expr
                         | operand kw_is kw_null         -> is_null
                         | operand kw_is kw_not kw_null   -> is_not_null

    SINGLE_QUOTED_STRING: /'(?:[^'\\]|\\.)*'/

    operand: CNAME
           | SIGNED_NUMBER
           | ESCAPED_STRING
           | kw_null
           | SINGLE_QUOTED_STRING

    COMPARISON_OP: ">" | "<" | "=" | ">=" | "<=" | "!=" | "<>"

    // Case-insensitive keywords
    kw_select: /[Ss][Ee][Ll][Ee][Cc][Tt]/
    kw_from: /[Ff][Rr][Oo][Mm]/
    kw_where: /[Ww][Hh][Ee][Rr][Ee]/
    kw_and: /[Aa][Nn][Dd]/
    kw_or: /[Oo][Rr]/
    kw_is: /[Ii][Ss]/
    kw_null: /[Nn][Uu][Ll][Ll]/
    kw_not: /[Nn][Oo][Tt]/
    kw_like: /[Ll][Ii][Kk][Ee]/

    LPAREN: "("
    RPAREN: ")"

    %import common.CNAME
    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
"""

class SQLTransformer(Transformer):
    def select_statement(self, items):
        return {
            'type': 'select',
            'columns': items[1],
            'table': items[3],
            'where': items[4] if len(items) > 4 else None
        }

    def column_list(self, items):
        if isinstance(items[0], Token) and items[0].type == 'ASTERISK':
            return ['*']
        return [item for item in items if not isinstance(item, Token) or item.type != ',']

    def column_name(self, items):
        return items[0].value

    def table_name(self, items):
        return items[0].value

    def where_clause(self, items):
        return items[1]

    def condition(self, items):
        return items[0]

    def expression(self, items):
        return items[0]

    def paren_expr(self, items):
        return items[1]

    def and_expr(self, items):
        return {
            'op': 'AND',
            'left': items[0],
            'right': items[2]
        }

    def or_expr(self, items):
        return {
            'op': 'OR',
            'left': items[0],
            'right': items[2]
        }

    def comparison_expression(self, items):
        return {
            'left_operand': items[0],
            'op': items[1].value if isinstance(items[1], Token) else items[1],
            'right_operand': items[2]
        }

    def is_null(self, items):
        return {
            'left_operand': items[0],
            'op': 'IS NULL'
        }

    def is_not_null(self, items):
        return {
            'left_operand': items[0],
            'op': 'IS NOT NULL'
        }

    def like_expr(self, items):
        return {
            'left_operand': items[0],
            'op': 'LIKE',
            'right_operand': items[2]
        }

    def operand(self, items):
        token = items[0]
        if isinstance(token, Token):
            match token.type:
                case "SIGNED_NUMBER":
                    try:
                        return int(token.value)
                    except ValueError:
                        return float(token.value)
                case "CNAME"| "ESCAPED_STRING" | "SINGLE_QUOTED_STRING":
                    return token.value
                case _:
                    return token.value
        elif token is None or (isinstance(token, str) and token.upper() == "NULL"):
            return None
        else:
            return token

    def kw_null(self, items):
        return None
