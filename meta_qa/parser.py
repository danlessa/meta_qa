import ast

def parse_text(text: str) -> dict:
    """
    Separates the text into an dict.
    """
    # Pre-processing
    if (text is None) or (len(text) == 0):
        return None
    if text[0] == '"':
        text = text[1:]
    if (text[-1] == '"'):
        text = text[:-1]

    # Get variables
    variables = text.split(";")
    output = {}
    for i, variable in enumerate(variables):
        if len(variable) == 0:
            continue
        if (i == 0):
            output["description"] = variable
        else:
            splitted_var = variable.replace("@", "").split("=")
            if len(splitted_var) >= 2:
                (key, value) = splitted_var[:2]
                # Parse lists and dicts
                if (value[0] == "[") or (value[0] == "{"):
                    value = ast.literal_eval(value)
                output[key] = value
    return output


def parse_task(raw_call: str) -> tuple:
    function = raw_call.split("(")[0]
    raw_args = raw_call.split("(")[1].split(")")[0].replace(" ", "").split(",")
    output = {"positional_args": [],
              "keyword_args": {}}
    for raw_arg in raw_args:
        kw_args = raw_arg.split("=")
        if len(kw_args) == 1:
            output['positional_args'].append(kw_args[0])
        elif len(kw_args) == 2:
            key = kw_args[0]
            value = kw_args[1]
            output["keyword_args"] = {key: value}
        else:
            continue
    return (function, output)
