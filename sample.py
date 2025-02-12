"""
Demo of main features of dataengtools module for data manipulation in AWS Glue.

Required dependencies:
- boto3: AWS SDK for Python
- polars: Data analysis library
- dataengtools: Internal module for data engineering operations
- numpy: Numerical computing library

Installation:
pip install boto3 polars numpy
"""
import polars as pl

from dataengtools.engine_factory import EngineFactory
from dataengtools.utils.logger import Logger

LOGGER = Logger.get_instance()

def main_catalog():    
    import logging
    import duckdb

    # Configuração do logging
    logging.basicConfig(
        format='%(asctime)s - %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configuração
    INPUT_FILEPATH = './input_data/input/100MB.fwf'
    #INPUT_FILEPATH = './input_data/input/1GB.fwf'
    #INPUT_FILEPATH = './input_data/input/10GB.fwf'
    #INPUT_FILEPATH = './input_data/input/30GB.fwf'
    
    fs = EngineFactory.get_filesystem_engine('duckdb|aws')

    # Obtendo a relação
    logging.info('Criando relação...')
    relation = fs.read_files(INPUT_FILEPATH, 'csv', {'has_header': False, 'separator': '|'})
    logging.info('Relação criada')

    batches = relation.fetch_arrow_reader(1_000_000)
    logging.info('Batches obtidos')

    for i, batch in enumerate(batches):
        logging.info(f'Inicializando batch {i}...')
        lf = pl.from_arrow(batch).lazy() #type: ignore

        logging.info(f'Processando batch {i}...')

        lf = lf.with_columns(
            pl.col('column0').str.slice(0, 10).alias('date'),
            pl.col('column0').str.slice(10, 50).alias('some_text'),
            pl.col('column0').str.slice(60, 30).alias('some_text2'),
            pl.col('column0').str.slice(90, 30).alias('some_text3'),
            pl.col('column0').str.slice(120, 30).alias('some_text4'),
            pl.col('column0').str.slice(150, 30).alias('some_text5'),
            pl.col('column0').str.slice(180, 30).alias('some_text6'),
            pl.col('column0').str.slice(210, 30).alias('some_text7'),
            pl.col('column0').str.slice(240, 30).alias('some_text8'),
            pl.col('column0').str.slice(270, 30).alias('some_text9'),
            pl.col('column0').str.slice(300, 400).alias('some_text10')
        )

        lf = lf.with_columns(
            pl.col('date').str.slice(0, 7).str.replace('-', '').alias('partition')
        )

        lf = lf.drop('column0')
        df = lf.collect()
        
        logging.info(f'Batch {i} processado. Iniciando escrita...')

        duckdb.register('source', df)
        duckdb.sql("COPY (SELECT * FROM source) TO './output_data/' (FORMAT PARQUET, PARTITION_BY (partition), APPEND)")
        duckdb.unregister('source')
        logging.info(f'Batch {i} processado e salvo.')




    #LOGGER.info('Data loaded successfully. Repairing table...')
    #catalog_engine.repair_table(OUTPUT_DATABASE, OUTPUT_TABLE)

    #LOGGER.info('Data ready to be queried.')

if __name__ == '__main__':
    main_catalog()