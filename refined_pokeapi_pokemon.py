import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import os
import sqlite3

class ReadAndDeserializeJson(beam.DoFn):
    """
    Um DoFn que desserializa uma string JSON (uma linha) em um objeto Python.
    """
    def process(self, element):
        try:
            yield json.loads(element)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON na linha: {element}. Erro: {e}")
            pass

class WriteToStructuredTable(beam.DoFn):
    """
    Um DoFn para inserir os dados extraídos do JSON em uma tabela estruturada SQLite.
    A gravação é centralizada para evitar o erro "database is locked".
    """
    def __init__(self, db_file):
        # Apenas o arquivo do banco de dados é necessário na inicialização
        self.db_file = db_file
        self.conn = None
        self.cur = None

    def setup(self):
        try:
            # Adiciona um timeout de 10 segundos
            self.conn = sqlite3.connect(self.db_file, timeout=10)
            self.cur = self.conn.cursor()
            print("Conexão com SQLite estabelecida.")
        except sqlite3.Error as e:
            raise RuntimeError(f"Erro na conexão com o banco de dados: {e}")

    def process(self, element):
        """
        Insere os elementos (registros JSON) na tabela especificada.
        O elemento de entrada é uma tupla (table_name, [registros])
        """
        table_name, records = element

        if not self.conn:
            print("Erro: Conexão com o banco de dados não estabelecida.")
            return

        try:
            for record in records:
                pkmn_id = record.get('id')
                name = record.get('name')
                height = record.get('height')
                weight = record.get('weight')
                base_experience = record.get('base_experience')
                is_default = record.get('is_default')
                order_number = record.get('order')
                species_name = record.get('species', {}).get('name')

                abilities_list = record.get('abilities', [])
                ability_name = abilities_list[0].get('ability', {}).get('name') if abilities_list else None

                types_list = record.get('types', [])
                type_name = types_list[0].get('type', {}).get('name') if types_list else None
                
                insert_sql = f"""
                    INSERT INTO {table_name} (id, name, height, weight, base_experience, is_default, order_number, species_name, ability_name, type_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                
                self.cur.execute(insert_sql, (pkmn_id, name, height, weight, base_experience, is_default, order_number, species_name, ability_name, type_name))
            
        except sqlite3.Error as e:
            print(f"Erro ao inserir registro na tabela {table_name}: {e}")
            self.conn.rollback() # Garante que a transação falhe se algo der errado
        
    def teardown(self):
        if self.conn:
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            print("Conexão com SQLite fechada.")

def create_table_if_not_exists(db_file, table_name):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT,
                name TEXT PRIMARY KEY,
                height INT,
                weight INT,
                base_experience INT,
                is_default INTEGER,
                order_number INT,
                species_name TEXT,
                ability_name TEXT,
                type_name TEXT
            );
        """
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        print(f"Tabela '{table_name}' verificada/criada com sucesso.")
    except sqlite3.Error as e:
        print(f"Erro ao criar tabela '{table_name}': {e}")
    finally:
        if conn:
            conn.close()

def add_table_name(element):
    """
    Função auxiliar para adicionar o nome da tabela ao elemento.
    """
    type_name = element.get('types', [{}])[0].get('type', {}).get('name')
    if type_name == 'grass':
        return ('pokemons_grass', element)
    elif type_name == 'fire':
        return ('pokemons_fire', element)
    elif type_name == 'water':
        return ('pokemons_water', element)
    else:
        return ('pokemons', element)

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    db_file = 'pokemons.db'
    
    tables_to_create = ['pokemons', 'pokemons_grass', 'pokemons_fire', 'pokemons_water']
    for table in tables_to_create:
        create_table_if_not_exists(db_file, table)

    with beam.Pipeline(options=options) as p:
        
        data = (p
                | 'Leitura dos arquivos' >> beam.io.ReadFromText('output_transformed/output_transformed_data.json-00000-of-00001'))
        
        deserialized_data = (data
                             | 'Desserializa JSON' >> beam.ParDo(ReadAndDeserializeJson()))

        # Adiciona o nome da tabela como a chave
        data_with_table = (deserialized_data
                            | 'Add Table Name' >> beam.Map(add_table_name))

        # Agrupa os dados pela chave (nome da tabela)
        grouped_by_table = (data_with_table
                            | 'Group by Table' >> beam.GroupByKey())

        # Envia os dados agrupados para a gravação, que será feita por uma única instância de worker por grupo
        grouped_by_table | 'Write to DB' >> beam.ParDo(WriteToStructuredTable(db_file))

if __name__ == '__main__':
    run()