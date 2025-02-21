class StringBuilder:
    """
    A simple utility class for incrementally building strings.

    This class accumulates string fragments and provides methods to append strings and to
    build the final string with a specified separator or with new lines.
    """
    
    def __init__(self):
        """
        Initialize a new instance of StringBuilder.

        Attributes:
            values (List[str]): An internal list to store appended string fragments.
        """
        self.values = []

    def append(self, value):
        """
        Append a string fragment to the builder.

        Parameters:
            value (str): The string fragment to append.

        Returns:
            StringBuilder: The instance of StringBuilder (self) to allow method chaining.
        """
        self.values.append(value)
        return self

    def build(self, separator: str = ' ', new_line: bool = False) -> str:
        """
        Build and return the final string composed from the appended fragments.

        Parameters:
            separator (str, optional): The separator to insert between fragments when joining.
                Defaults to a single space (' ').
            new_line (bool, optional): If True, the fragments are joined using newline characters.
                If False, the specified separator is used. Defaults to False.

        Returns:
            str: The concatenated string.
        """
        if new_line:
            return '\n'.join(self.values)
        return separator.join(self.values)