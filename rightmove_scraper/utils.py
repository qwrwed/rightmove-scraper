import re

def snake_to_camel_case(snake_str: str) -> str:
    return re.sub(r"_([a-zA-Z])", lambda x: x.group(1).upper(), snake_str)
