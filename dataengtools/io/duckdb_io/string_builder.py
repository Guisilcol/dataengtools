class StringBuilder:
    def __init__(self):
        self.values = []

    def append(self, value):
        self.values.append(value)
        return self

    def build(self, separator: str = ' ', new_line: bool = False) -> str:
        if new_line:
            return '\n'.join(self.values)
        return separator.join(self.values)
