from typing import List, Optional
from urllib.parse import urlparse
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client 
from dataengtools.interfaces.metadata import Partition, PartitionHandler

class AWSGluePartitionHandler(PartitionHandler):
    def __init__(self, glue: GlueClient, s3: S3Client) -> None:
        self.glue = glue
        self.s3 = s3

    def get_partitions(self, database: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        paginator = self.glue.get_paginator('get_partitions')
        partitions = []
        
        table_response = self.glue.get_table(DatabaseName=database, Name=table)
        base_location = table_response['Table']['StorageDescriptor']['Location'].rstrip('/')
        table_name = table_response['Table']['Name']
        
        if conditions is None:
            pages = paginator.paginate(DatabaseName=database, TableName=table)
        else:
            pages = paginator.paginate(DatabaseName=database, TableName=table, Expression=conditions)
        
        for page in pages:
            for partition in page.get('Partitions', []):
                location = partition['StorageDescriptor']['Location'].rstrip('/')
                values = partition['Values']
                raw_metadata = partition

                # Extrair o "nome" da partição a partir da localização removendo o prefixo da localização base da tabela.
                if location.startswith(base_location):
                    name = location[len(base_location):].strip('/')
                else:
                    name = location
                    
                name = table_name + '/' + name
                    
                bucket, _ = location.replace('s3://', '').split('/', 1)

                partitions.append(
                    Partition(
                        name=name,
                        location=location,
                        values=values,
                        root=bucket,
                        raw_metadata=raw_metadata
                    )
                )
                
        return partitions

    def delete_partitions(self, database: str, table: str, partitions: List[Partition]) -> None:
        CHUNK_SIZE = 25
        for i in range(0, len(partitions), CHUNK_SIZE):
            batch = partitions[i:i+CHUNK_SIZE]
            partitions_to_delete = [{'Values': p.values} for p in batch]

            self.glue.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=partitions_to_delete
            )

    def repair_table(self, database: str, table: str) -> None:
        """
        Repair the table in the database:
        - Remove partitions that exist in the Glue Catalog but not in the filesystem.
        - Add partitions that exist in the filesystem but not in the Glue Catalog.

        :param database: The name of the database.
        :param table: The name of the table.
        """
        # Obter detalhes da tabela
        table_response = self.glue.get_table(DatabaseName=database, Name=table)
        table_sd = table_response['Table']['StorageDescriptor']
        base_location = table_sd['Location'].rstrip('/')
        partition_keys = table_response['Table'].get('PartitionKeys', [])
        
        # Partições já registradas no Glue
        existing_partitions = self.get_partitions(database, table)
        existing_partition_values = {tuple(p.values) for p in existing_partitions}

        # Inferir a estrutura das partições a partir das partition_keys
        # Ex: se partition_keys = [{'Name': 'dt'}, {'Name': 'region'}]
        # Esperamos caminhos do tipo dt=2020-01-01/region=US
        pk_names = [pk['Name'] for pk in partition_keys]

        # Caso não haja chaves de partição, não há o que reparar
        if not pk_names:
            return

        # Listar todos os potenciais diretórios de partições no S3
        # Assumindo que cada partição é um diretório e possui pelo menos um arquivo dentro.
        parsed = urlparse(base_location)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')

        # Vamos listar todos os objetos no caminho base. Poderemos filtrar somente pastas.
        # Para não gerar tráfego excessivo, supõe-se que o table_location não seja gigante.
        # Em um cenário real, pode ser necessário refinar ou usar um approach diferente.
        paginator = self.s3.get_paginator('list_objects_v2')
        
        s3_partition_paths = set()
        
        # Estratégia: Para identificar partições, usaremos a convenção "key=value"
        # por nível de partição. Iteramos sobre todos os objetos e extraímos o caminho 
        # até as chaves de partição. Cada objeto pode estar em um caminho do tipo:
        # s3://bucket/base_location/key1=value1/key2=value2/.../datafile
        # Vamos coletar todos os caminhos de partição (sem o nome do arquivo final).
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Remover o prefixo base (prefix) do caminho
                relative_path = key[len(prefix):].lstrip('/')
                # O relative_path pode ser algo como: dt=2020-01-01/region=US/datafile.csv
                # Queremos extrair apenas a parte de "dt=2020-01-01/region=US"
                # Isso significa remover a última parte se for um arquivo. Por segurança,
                # listaremos todos os níveis até encontrar um padrão que corresponda
                # exatamente ao número de partition_keys.

                parts = relative_path.split('/')
                
                # Tentar identificar quantos níveis de partição existem.
                # Se temos N partition_keys, vamos extrair os primeiros N níveis.
                if len(parts) < len(pk_names):
                    # Não há partições completas nesse caminho
                    continue
                
                candidate_parts = parts[:len(pk_names)]
                # Checar se cada parte está no formato key=value
                valid_structure = True
                values_extracted = []
                for pk, cpart in zip(pk_names, candidate_parts):
                    if '=' not in cpart:
                        valid_structure = False
                        break
                    key_name, key_val = cpart.split('=', 1)
                    if key_name != pk:
                        valid_structure = False
                        break
                    values_extracted.append(key_val)
                
                if valid_structure:
                    # Temos um conjunto de valores de partição válido
                    # Vamos adicionar ao conjunto
                    s3_partition_paths.add(tuple(values_extracted))

        # Agora temos:
        # existing_partition_values: partições no Glue
        # s3_partition_paths: partições no S3 (descobertas)

        # Partições no Glue que não estão no S3 → remover
        to_remove = existing_partition_values - s3_partition_paths
        # Partições no S3 que não estão no Glue → criar
        to_add = s3_partition_paths - existing_partition_values

        # Remover partições inexistentes no S3
        if to_remove:
            # Obter os objetos Partition correspondentes a partir do existing_partitions
            remove_partitions_obj = [p for p in existing_partitions if tuple(p.values) in to_remove]
            self.delete_partitions(database, table, remove_partitions_obj)

        # Criar partições que estão no S3 mas não no Glue
        # Para criar partições, precisamos usar batch_create_partition.
        # Precisamos construir a StorageDescriptor. Podemos basear no table_sd.
        # Cada partição terá seu location: base_location + "/key1=value1/key2=value2/..."
        # Precisamos gerar esse sufixo a partir de to_add.

        def build_partition_location(values_list):
            parts = [f"{k}={v}" for k, v in zip(pk_names, values_list)]
            return f"{base_location}/{'/'.join(parts)}"

        # Dividir em chunks de 25
        CHUNK_SIZE = 25
        to_add_list = list(to_add)
        for i in range(0, len(to_add_list), CHUNK_SIZE):
            batch = to_add_list[i:i+CHUNK_SIZE]

            # Construir lista de partições para criação
            # Precisamos de: Values e StorageDescriptor
            # O StorageDescriptor pode ser copiado do da tabela, modificando somente o Location.
            # Remover fields não necessários. É recomendável remover `Location` do original e setar o correto.

            partitions_to_create = []
            for vals in batch:
                part_location = build_partition_location(vals)
                # Clonar o storage descriptor da tabela e ajustar localização:
                new_sd = {
                    "Columns": table_sd["Columns"],
                    "Location": part_location,
                    "InputFormat": table_sd["InputFormat"],
                    "OutputFormat": table_sd["OutputFormat"],
                    "Compressed": table_sd.get("Compressed", False),
                    "NumberOfBuckets": table_sd.get("NumberOfBuckets", 0),
                    "SerdeInfo": table_sd.get("SerdeInfo", {}),
                    "BucketColumns": table_sd.get("BucketColumns", []),
                    "SortColumns": table_sd.get("SortColumns", []),
                    "Parameters": table_sd.get("Parameters", {}),
                    "SkewedInfo": table_sd.get("SkewedInfo", {})
                }

                partitions_to_create.append({
                    "Values": list(vals),
                    "StorageDescriptor": new_sd
                })

            self.glue.batch_create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInputList=partitions_to_create
            )
