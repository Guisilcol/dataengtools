from typing import Tuple
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition



class PartitionHelper:

    @staticmethod
    def get_values_from_partition(partition: Partition) -> Tuple[str]:
        """Get only the values from a partition string. Input ex.: 'key1=value1/key2=value2', output: ['value1', 'value2']"""
        values = partition.split('/')
        return tuple([v.split('=')[1] for v in values])