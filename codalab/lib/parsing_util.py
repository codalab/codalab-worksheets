import functools
from typing import Callable

from lark import Lark, Transformer, v_args

REQUEST_QUEUE_GRAMMAR = r"""
    ?start: or_expr
    ?or_expr: and_expr ("|" and_expr)*
    ?and_expr: atom ("&" atom)*
    ?atom: ALPHANUMERIC -> name
        | "!" atom -> not_expr
        | "(" or_expr ")"

    ALPHANUMERIC: (LETTER|DIGIT)+
    %import common.LETTER
    %import common.DIGIT
    %import common.WS
    %ignore WS
"""


@v_args(inline=True)
class FunctionTransformer(Transformer):
    def or_expr(self, *exprs):
        return f"({' or '.join(exprs)})"

    def and_expr(self, *exprs):
        return f"({' and '.join(exprs)})"

    def not_expr(self, atom):
        return f" not {atom}"

    def name(self, n):
        return f"'{n}' in worker_tag"


class RequestQueueParser:
    def __init__(self):
        self.parser = Lark(REQUEST_QUEUE_GRAMMAR)
        self.function_transformer = FunctionTransformer()

    @functools.lru_cache(maxsize=128, typed=False)
    def parse_request_queue_to_callable(self, request_queue: str) -> Callable:
        tree = self.parser.parse(request_queue)
        function_string = self.function_transformer.transform(tree)
        # NOTE: this is potentially dangerous. The parser is designed to only accept alphanumeric input
        # (excepting its operators). Similarly, self.collection_name should be alphanumeric.
        # These two constraints make it hard to call functions or do bad things with double-underscores.
        return eval(f"lambda worker_tag : {function_string}")
