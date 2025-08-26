import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import requests
import json
import time

# URL da API
API_URL = "https://pokeapi.co/api/v2/pokemon"

class ConnAPI(beam.DoFn):
    """
    Um DoFn para buscar dados de uma API paginada e coletar todos os registros.
    """
    def process(self, element):
        all_results = []
        next_url = API_URL
        
        while next_url:
            max_retries = 3
            retries = 0
            success = False
            
            while retries < max_retries and not success:
                try:
                    response = requests.get(next_url, timeout=30)  # Adiciona um tempo limite
                    response.raise_for_status()
                    data = response.json()
                    
                    all_results.extend(data.get("results", []))
                    next_url = data.get("next")
                    success = True

                except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                    retries += 1
                    print(f"Erro ao buscar dados da API (Tentativa {retries}/{max_retries}): {e}")
                    if retries < max_retries:
                        print("Tentando novamente em 5 segundos...")
                        time.sleep(5)
                    else:
                        print("Falha na conexão após várias tentativas. Desistindo.")
                        next_url = None # Para o loop em caso de erro persistente

        if all_results:
            yield all_results

class IngestionAndSave(beam.DoFn):
    """
    Um DoFn para processar e salvar cada registro.
    """
    def process(self, records):
        for record in records:
            # Faz uma chamada à API para obter os detalhes completos de cada Pokémon
            try:
                details_response = requests.get(record['url'], timeout=30)
                details_response.raise_for_status()
                details_data = details_response.json()
                yield details_data
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                print(f"Erro ao buscar detalhes para {record.get('name', 'N/A')}: {e}")
                pass

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'

    with beam.Pipeline(options=options) as p:
        api_data = (p
                    | 'Gatilho da pipeline' >> beam.Create([None])
                    | 'Conexão com API e Paginação' >> beam.ParDo(ConnAPI()))

        detailed_data = (api_data
                         | 'Ingestão e Detalhes' >> beam.ParDo(IngestionAndSave()))

        detailed_data | 'Serializar para JSON' >> beam.Map(json.dumps) \
                    | 'Gravar no arquivo' >> beam.io.WriteToText('output_raw/output_raw_data.json')

if __name__ == '__main__':
    run()