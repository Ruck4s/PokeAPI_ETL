import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import os

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

class CleaningFiles(beam.DoFn):
    """
    Um DoFn para remover chaves específicas e tratar campos nulos.
    """
    def _remove_keys_recursively(self, data, keys_to_remove):
        if isinstance(data, dict):
            return {key: self._remove_keys_recursively(value, keys_to_remove)
                    for key, value in data.items()
                    if key not in keys_to_remove}
        elif isinstance(data, list):
            cleaned_list = [self._remove_keys_recursively(item, keys_to_remove)
                            for item in data]
            return [item for item in cleaned_list if item is not None]
        else:
            return data

    def _handle_nulls_recursively(self, data):
        if isinstance(data, dict):
            return {key: self._handle_nulls_recursively(value) 
                    for key, value in data.items()
                    if value not in ("", [], {}) or self._handle_nulls_recursively(value) is not None}
        elif isinstance(data, list):
            cleaned_list = [self._handle_nulls_recursively(item) 
                            for item in data]
            return [item for item in cleaned_list if item is not None]
        elif data in ("", [], {}):
            return None
        else:
            return data

    def process(self, element):
        keys_to_remove = [
            "cries",
            "game_indices",
            "location_area_encounters",
            "held_items",
            "version_group_details",
            "versions"
        ]
        
        # 1. Remove as chaves indesejadas
        cleaned_element = self._remove_keys_recursively(element, keys_to_remove)
        
        # 2. Trata os campos nulos
        final_element = self._handle_nulls_recursively(cleaned_element)
        
        if final_element:
            yield final_element

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    # Cria o diretório de saída se ele não existir
    output_dir = 'output_transformed'
    output_filename = 'output_transformed_data.json'
    os.makedirs(output_dir, exist_ok=True)

    with beam.Pipeline(options=options) as p:
        
        # 1. Lê o arquivo JSONL linha por linha
        data = (p
                    | 'Leitura dos arquivos' >> beam.io.ReadFromText('output_raw/*'))
        
        # 2. Desserializa cada linha lida em um objeto Python
        deserialized_data = (data
                             | 'Desserializa JSON' >> beam.ParDo(ReadAndDeserializeJson()))

        # 3. Limpa os dados em cada objeto
        cleaned_data = (deserialized_data
                        | 'Limpeza dos Dados' >> beam.ParDo(CleaningFiles()))

        # 4. Serializa o objeto limpo de volta para JSON e salva
        (cleaned_data
            | 'Serializar para JSON' >> beam.Map(json.dumps)
            | 'Gravar no arquivo' >> beam.io.WriteToText(os.path.join(output_dir, output_filename)))

if __name__ == '__main__':
    run()