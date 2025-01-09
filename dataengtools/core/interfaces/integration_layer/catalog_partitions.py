from abc import ABC, abstractmethod
from typing import List, Optional, NewType

Partition = NewType('Partition', str)
"""
A type representing a partition in a Hive table.
This type is used to define a partition string, which typically follows the format used in Hive: `key1=value1/key2=value2`.
"""


class PartitionHandler(ABC):
    """
    Abstract base class for handling table partitions.
    """
    @abstractmethod
    def get_partitions(self, database: str, table: str, conditions: Optional[str], additional_configs: dict = {}) -> List[Partition]:
        """
        Retrieve partitions for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :param conditions: Optional conditions to filter partitions.
        :return: List of Partition objects.
        """
        pass
    
    @abstractmethod
    def delete_partitions(self, database: str, table: str, partition: List[Partition], additional_configs: dict = {}) -> None:
        """
        Delete specified partitions from a table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :param partition: List of Partition objects to be deleted.
        """
        pass
    
    @abstractmethod
    def repair_table(self, database: str, table: str, additional_configs: dict = {}) -> None:
        """
        Repair the table in the database.

        :param database: The name of the database.
        :param table: The name of the table.
        """
        pass

