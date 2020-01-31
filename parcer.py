class ParsecCSV:
    def __init__(self, line):
        line = line.split(",")
        self._name = line[0]
        self._shop = int(line[1])
        self._traffic = int(line[2])
        self._success_sales = int(line[3])

    @property
    def get_name(self):
        return self._name

    @property
    def get_shop(self):
        return self._shop

    @property
    def get_traffic(self):
        return self._traffic

    @property
    def get_success_sales(self):
        return self._success_sales

    @staticmethod
    def parce_CSV(line: str) -> tuple:
        line = line.split(",")
        return line[0], line[1], line[2], line[3]
