import json


class Builder:
    """
    Class for abstract factory for input format.

    Available format is: "CSV", "JSON" and "AVRO"
    """

    def __init__(self, TYPE):
        """ Initialization input format """
        self.type = TYPE.upper()

    def get_builder(self):
        """ Method for getting builder by format """
        if self.type == "CSV":
            return BuilderCSV
        elif self.type == "JSON":
            return BuilderJSON
        elif self.type == "AVRO":
            return BuilderAVRO
        else:
            pass


class BuilderCSV:
    """ Class of builder for CSV format """

    @staticmethod
    def build_message(name, shop, traffic, success_sell):
        """ Method for building a message to send """
        return ",".join([name, str(shop), str(traffic), str(success_sell)])

    @staticmethod
    def serializer():
        return str.encode


class BuilderJSON:
    """ Class of builder for JSON format """

    @staticmethod
    def build_message(name, shop, traffic, success_sell):
        """ Method for building a message to send """
        data = dict(name=name, shop=shop, traffic=traffic, success_sell=success_sell)
        return json.dumps(data)

    @staticmethod
    def serializer():
        return str.encode


class BuilderAVRO:
    """ Class of builder for AVRO format """

    @staticmethod
    def build_message(name, shop, traffic, success_sell):
        """ Method for building a message to send """
        pass

    @staticmethod
    def serializer():
        pass
