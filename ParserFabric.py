class Parser:

    def __init__(self, TYPE):
        self.type = TYPE

    def get_parser(self):
        if self.type.upper() == "CSV":
            return ParserCSV
        elif self.type.upper() == "JSON":
            pass
        elif self.type.upper() == "AVRO":
            pass
        else:
            pass


class ParserCSV:

    @staticmethod
    def parse(line):
        line = line.split(",")
        return line[0], int(line[1]), int(line[2]), int(line[3])


class ParserJSON:

    @staticmethod
    def parse(line):
        pass


class ParserAVRO:

    @staticmethod
    def parse(line):
        pass
