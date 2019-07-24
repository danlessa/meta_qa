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
            output["description_text"] = variable
        else:
            splitted_var = variable.replace("@", "").split("=")
            if len(splitted_var) >= 2:
                (key, value) = splitted_var[:2]
                # Parse lists and dicts
                if (value[0] == "[") or (value[0] == "{"):
                    try:
                        value = ast.literal_eval(value)
                    except:
                        value = None
                output[key] = value
    return output


def parse_task(raw_call: str) -> tuple:
    """
    Parses an raw function call into an tuple containing
    function name and args.
    """
    # Initial check-up
    if type(raw_call) != str:
        return None
    split_call = raw_call.split("(")
    if len(split_call) != 2:
        return None

    function = split_call[0]
    raw_args = (split_call[1].split(")")[0]
                             .replace(" ", "")
                             .split(","))

    output = {"positional_args": [],
              "keyword_args": {}}

    for raw_arg in raw_args:
        kw_args = raw_arg.split("=")
        if len(kw_args) == 1:
            if len(kw_args[0]) > 0:
                output['positional_args'].append(kw_args[0])
        elif len(kw_args) == 2:
            key = kw_args[0]
            value = kw_args[1]
            output["keyword_args"] = {key: value}
        else:
            continue
    return (function, output)
