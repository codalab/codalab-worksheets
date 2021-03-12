import unittest
from codalab.lib import parsing_util

import lark
from lark import Lark, Token
from lark.tree import Tree

class ParsingUtilTest(unittest.TestCase):
    """
    This class tests parsing_util.REQUEST_QUEUE_GRAMMAR and
    parsing_util.FunctionTransformer . The logic that matches
    request_queue values to workers is tested in the BundleManager tests.
    """
    def test_grammar_basic(self):
        parser = Lark(parsing_util.REQUEST_QUEUE_GRAMMAR)
        # Test single item
        expected_tag1_tree = Tree('name', [Token('ALPHANUMERIC', 'tag1')])
        expected_tag2_tree = Tree('name', [Token('ALPHANUMERIC', 'tag2')])
        expected_tag3_tree = Tree('name', [Token('ALPHANUMERIC', 'tag3')])
        self.assertEqual(parser.parse("tag1"), expected_tag1_tree)
        # Test negation of single item
        self.assertEqual(parser.parse("!tag1"), Tree('not_expr', [expected_tag1_tree]))
        # Test or of two items
        self.assertEqual(parser.parse("tag1 | tag2"), Tree('or_expr', [expected_tag1_tree, expected_tag2_tree]))
        # Test and of two items
        self.assertEqual(parser.parse("tag1 & tag2"), Tree('and_expr', [expected_tag1_tree, expected_tag2_tree]))
        # Test nested expressions
        self.assertEqual(parser.parse("tag1 | (tag2 & tag3)"),
                         Tree('or_expr', [
                             expected_tag1_tree,
                             Tree('and_expr', [expected_tag2_tree, expected_tag3_tree])]))
        self.assertEqual(parser.parse("tag1 & (tag2 | tag3)"),
                         Tree('and_expr',[
                             expected_tag1_tree,
                             Tree('or_expr', [expected_tag2_tree, expected_tag3_tree])]))
        # Test nesting with parens. Precedence (highest to lowest) is not, and, or.
        self.assertEqual(parser.parse("!tag1 & tag2 | tag3"),
                         Tree('or_expr', [
                             Tree('and_expr', [
                                 Tree('not_expr', [expected_tag1_tree]),
                                 expected_tag2_tree]),
                             expected_tag3_tree]))
        self.assertEqual(parser.parse("!tag1 & tag2 | tag3"),
                         parser.parse("(!tag1) & tag2 | tag3"))
        self.assertEqual(parser.parse("!tag1 & (tag2 | tag3)"),
                         Tree('and_expr', [
                             Tree('not_expr', [expected_tag1_tree]),
                             Tree('or_expr', [expected_tag2_tree, expected_tag3_tree])]))
        self.assertEqual(parser.parse("!(tag1 & tag2) | tag3"),
                         Tree('or_expr', [
                             Tree('not_expr', [
                                 Tree('and_expr', [
                                     expected_tag1_tree,
                                     expected_tag2_tree])]),
                             expected_tag3_tree]))
        self.assertEqual(parser.parse("!(tag1 & (tag2 | tag3))"),
                         Tree('not_expr', [
                             Tree('and_expr', [
                                 expected_tag1_tree,
                                 Tree('or_expr', [expected_tag2_tree, expected_tag3_tree])])]))


    def test_grammar_invalid(self):
        parser = Lark(parsing_util.REQUEST_QUEUE_GRAMMAR)
        with self.assertRaises(lark.exceptions.UnexpectedCharacters):
            parser.parse("tag_1")
            parser.parse("not tag_1")
        with self.assertRaises(lark.exceptions.UnexpectedEOF):
            parser.parse("tag1 & tag2 |")
            parser.parse("(tag1 & tag2")

    def test_transformer_basic(self):
        parser = Lark(parsing_util.REQUEST_QUEUE_GRAMMAR)
        transformer = parsing_util.FunctionTransformer()
        # Test single item
        self.assertEqual(transformer.transform(parser.parse("tag1")), "'tag1' in worker_tags")
        # Test negation of single item
        self.assertEqual(transformer.transform(parser.parse("!tag1")), "not ('tag1' in worker_tags)")
        # Test or of two items
        self.assertEqual(transformer.transform(parser.parse("tag1 | tag2")),
                         "('tag1' in worker_tags or 'tag2' in worker_tags)")
        # Test and of two items
        self.assertEqual(transformer.transform(parser.parse("tag1 & tag2")),
                         "('tag1' in worker_tags and 'tag2' in worker_tags)")
        # Test nested eworker_tagspressions
        self.assertEqual(transformer.transform(parser.parse("tag1 | (tag2 & tag3)")),
                         "('tag1' in worker_tags or ('tag2' in worker_tags and 'tag3' in worker_tags))")
        self.assertEqual(transformer.transform(parser.parse("tag1 & (tag2 | tag3)")),
                         "('tag1' in worker_tags and ('tag2' in worker_tags or 'tag3' in worker_tags))")
        # Test nesting with parens. Precedence (highest to lowest) isnot, and, or.
        self.assertEqual(transformer.transform(parser.parse("!tag1 & tag2 | tag3")),
                         "((not ('tag1' in worker_tags) and 'tag2' in worker_tags) or 'tag3' in worker_tags)")
        self.assertEqual(transformer.transform(parser.parse("(!tag1) & tag2 | tag3")),
                         "((not ('tag1' in worker_tags) and 'tag2' in worker_tags) or 'tag3' in worker_tags)")
        self.assertEqual(transformer.transform(parser.parse("!tag1 & (tag2 | tag3)")),
                         "(not ('tag1' in worker_tags) and ('tag2' in worker_tags or 'tag3' in worker_tags))")
        self.assertEqual(transformer.transform(parser.parse("!(tag1 & tag2) | tag3")),
                         "(not (('tag1' in worker_tags and 'tag2' in worker_tags)) or 'tag3' in worker_tags)")
        self.assertEqual(transformer.transform(parser.parse("!(tag1 & (tag2 | tag3))")),
                         "not (('tag1' in worker_tags and ('tag2' in worker_tags or 'tag3' in worker_tags)))")
