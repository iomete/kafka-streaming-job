from abc import abstractmethod


class DataSync:

    @abstractmethod
    def sync(self):
        """
        Responsible for processing spark streaming data synchronisation

        :return:
        """
        pass
