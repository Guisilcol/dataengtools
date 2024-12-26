import polars as pl
from dataengtools.interfaces.datatype_mapping import DataTypeMapping

class AWSGlueDataTypeToPolars(DataTypeMapping[str, pl.DataType]):
    MAPPING = {
        'string': pl.Utf8,          # Correto: Texto UTF-8
        'int': pl.Int64,            # Correto: Inteiro de 64 bits
        'bigint': pl.Int64,         # Ajustado para Int64 (inteiro de 64 bits)
        'double': pl.Float64,       # Ajustado para Float64
        'float': pl.Float32,        # Ajustado para Float32
        'boolean': pl.Boolean,      # Ajustado para Boolean
        'timestamp': pl.Utf8,   # Ajustado para Datetime
        'date': pl.Utf8,            # Ajustado para Date
        'decimal': pl.Float64,      # Decimal representado como Float64
        'array': pl.List,           # Arrays mapeados para List
        'map': pl.Object,           # Map mapeado para Object
        'struct': pl.Object,        # Struct mapeado para Object
        'binary': pl.Binary         # Ajustado para Binary
    }