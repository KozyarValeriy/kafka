import json


class Parser:
    """
    Class for abstract factory for output format.

    Available format is: "CSV", "JSON" and "AVRO"
    """

    def __init__(self, TYPE):
        """ Initialization input format """
        self.type = TYPE.upper()

    def get_parser(self):
        """ Method for getting parser by format """
        if self.type == "CSV":
            return ParserCSV
        elif self.type == "JSON":
            return ParserJSON
        elif self.type == "AVRO":
            return ParserAVRO
        else:
            pass


class ParserCSV:
    """ Class parser for CSV format """

    @staticmethod
    def parse(line):
        """ Method for parsing a message """
        line = line.split(",")
        return line[0], int(line[1]), int(line[2]), int(line[3])

    @staticmethod
    def deserializer():
        return bytes.decode


class ParserJSON:
    """ Class parser for JSON format """

    @staticmethod
    def parse(line):
        """ Method for parsing a message """
        data = json.loads(line)
        return data['name'], data['shop'], data['traffic'], data['success_sell']

    @staticmethod
    def deserializer():
        return bytes.decode


class ParserAVRO:
    """ Class parser for AVRO format """

    @staticmethod
    def parse(line):
        """ Method for parsing a message """
        pass

    @staticmethod
    def deserializer():
        pass
