from meta_qa.parser import parse_task, parse_text


class TestParser(object):

    def test_text_parsing(self):
        input = "description;@var1=text;@var2=['item1', 'item2'];@var3={'key': 'value'}"
        expected_output = {"description_text": "description",
                           "var1": "text",
                           "var2": ['item1', 'item2'],
                           "var3": {'key': 'value'}}
        assert (parse_text(input) == expected_output)

        input = "description;@var1=text;@var2=['item1', 'item2];@var3={'key': 'value']"
        expected_output = {"description_text": "description",
                           "var1": "text",
                           "var2": None,
                           "var3": None}
        assert (parse_text(input) == expected_output)

    def test_call_parsing(self):
        input = "function(text, 0.05, 10, key1=value1)"
        expected_output = ('function',
                           {"positional_args": ['text', '0.05', '10'],
                            "keyword_args": {"key1": "value1"}})

        assert (parse_task(input) == expected_output)

        input = "randomtext"
        expected_output = None

        assert (parse_task(input) == expected_output)
