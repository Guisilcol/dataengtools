from abc import ABC, abstractmethod
from typing import List, Optional, NewType

Partition = NewType('Partition', str)
"""
A string type representing a partition in a Hive table.
Typically follows the Hive format: "key1=value1/key2=value2".
"""

class PartitionHandler(ABC):
    """
    Abstract base class for handling table partitions.

    This class provides an interface for operations on table partitions,
    including retrieving, deleting, and repairing partitions in a Hive-style table.
    """

    @abstractmethod
    def get_partitions(
        self, 
        database: str, 
        table: str, 
        conditions: Optional[str] = None, 
        additional_configs: dict = {}
    ) -> List[Partition]:
        """
        Retrieve partitions for a specific table in a database.

        Parameters:
            database (str): The name of the database.
            table (str): The name of the table.
            conditions (Optional[str]): Optional filter conditions for the partitions.
            additional_configs (dict, optional): Additional configuration parameters.

        Returns:
            List[Partition]: A list of partitions that match the criteria.
        """
        pass
    
    @abstractmethod
    def delete_partitions(
        self, 
        database: str, 
        table: str, 
        partition: List[Partition], 
        additional_configs: dict = {}
    ) -> None:
        """
        Delete specified partitions from a table in the database.

        Parameters:
            database (str): The name of the database.
            table (str): The name of the table.
            partition (List[Partition]): A list of partitions to be deleted.
            additional_configs (dict, optional): Additional configuration parameters.

        Returns:
            None
        """
        pass
    
    @abstractmethod
    def repair_table(
        self, 
        database: str, 
        table: str, 
        additional_configs: dict = {}
    ) -> None:
        """
        Repair the metadata of a table in the database by refreshing its partition information.

        Parameters:
            database (str): The name of the database.
            table (str): The name of the table.
            additional_configs (dict, optional): Additional configuration parameters.

        Returns:
            None
        """
        pass