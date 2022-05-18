from abc import abstractmethod


class DataSync:

    @abstractmethod
    def process(self):
        """
        Responsible for processing spark streaming
        :return:
        """
        pass
