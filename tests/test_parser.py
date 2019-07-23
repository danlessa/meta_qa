from meta_qa.parser import parse_text
from meta_qa.parser import parse_task


class TestParser(object):

    def test_text_parsing(self):
        input = "description;@var1=text;@var2=['item1', 'item2'];@var3={'key': 'value'}"
        expected_output = {"description": "description",
                           "var1": "text",
                           "var2": ['item1', 'item2'],
                           "var3": {'key': 'value'}}
        assert (parse_text(input) == expected_output)


    def test_call_parsing(self):
        input = "function(text, 0.05, 10, key1=value1)"
        expected_output = ('function',
                           {"positional_args": ['text', '0.05', '10'],
                            "keyword_args": {"key1": "value1"}})
        assert parse_task(input) == expected_output
