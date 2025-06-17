import requests
import csv
from typing import List, Dict
from datetime import datetime
import time
import os
import logging
from dotenv import load_dotenv

load_dotenv()

def setup_logging():
      if not os.path.exists('logs'):
            os.makedirs('logs')
      
      timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
      log_file = f'logs/batch_process_{timestamp}.log'
      
      logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                  logging.FileHandler(log_file),
                  logging.StreamHandler()  
            ]
      )
      return log_file

def make_api_call(url: str, token: str) -> Dict:
        headers = {
                  'Authorization': f'Bearer {token}',
                  'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
                  response_data = response.json()
                  if 'data' in response_data and isinstance(response_data['data'], list) and len(response_data['data']) > 0:
                          return {
                                    'success': True,
                                    'data': response_data['data'],
                                    'count': len(response_data['data'])
                          }
                  else:
                          return {
                                    'success': False,
                                    'message': 'Nenhum dado encontrado ou os dados estão vazios'
                          }
        else:
                  return {
                          'success': False,
                          'error': f"Requisição falhou com código de status {response.status_code}"
                  }

def process_ids_from_csv(csv_path: str, token: str, base_url: str) -> List[Dict]:
      results = []
      total_ids = 0
      processed = 0
      start_time = time.time()
      
      logging.info("Iniciando processamento em lote de IDs de avaliações...")
      
      try:
            with open(csv_path, 'r') as file:
                  total_ids = sum(1 for row in csv.reader(file) if row)
            
            logging.info(f"Encontrados {total_ids} IDs para processar")
            
            with open(csv_path, 'r') as file:
                  csv_reader = csv.reader(file)
                  for row in csv_reader:
                        if not row:  
                              continue
                        
                        processed += 1
                        evaluation_id = row[0].strip()
                        logging.info(f"Processando ID {evaluation_id} ({processed}/{total_ids})")
                        
                        url = f"{base_url}/{evaluation_id}/results"
                        result = make_api_call(url, token)
                        
                        result['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        results.append({
                              'id': evaluation_id,
                              **result
                        })
                        
                        if result['success']:
                              logging.info(f"Sucesso! Encontrados {result['count']} itens na resposta para o ID {evaluation_id}")
                        else:
                              logging.error(f"Falha ao processar ID {evaluation_id}: {result.get('message', result.get('error', 'Erro desconhecido'))}")
                        
                        logging.info(f"Progresso: {processed}/{total_ids} ({(processed/total_ids*100):.1f}%)")
      
      except FileNotFoundError:
            logging.error(f"Arquivo CSV '{csv_path}' não encontrado")
      except Exception as e:
            logging.error(f"Erro ao processar CSV: {str(e)}")
      
      elapsed_time = time.time() - start_time
      
      return results, elapsed_time

def log_summary(results: List[Dict], elapsed_time: float, csv_path: str):
      successful = sum(1 for r in results if r['success'])
      failed = len(results) - successful
      
      logging.info("\n=== Resumo do Processamento em Lote ===")
      logging.info(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
      logging.info(f"Arquivo de entrada: {csv_path}")
      logging.info(f"Tempo de processamento: {elapsed_time:.2f} segundos")
      logging.info(f"Total de IDs processados: {len(results)}")
      logging.info(f"Sucessos: {successful}")
      logging.info(f"Falhas: {failed}")
      logging.info(f"Taxa de sucesso: {(successful/len(results)*100 if results else 0):.1f}%")
      
      if failed > 0:
            logging.info("\nIDs com falha:")
            for result in results:
                  if not result['success']:
                        logging.error(f"ID {result['id']}: {result.get('message', result.get('error', 'Erro desconhecido'))}")
      
      logging.info("Processamento concluído!")

def main():
      bearer_token = os.getenv('BEARER_TOKEN')
      base_url = os.getenv('BASE_URL')
      csv_path = os.getenv('CSV_PATH')
      
      if not all([bearer_token, base_url, csv_path]):
            logging.error("Missing required environment variables. Please check your .env file")
            return
      
      log_file = setup_logging()
      logging.info(f"Log file created at: {log_file}")
      
      results, elapsed_time = process_ids_from_csv(csv_path, bearer_token, base_url)
      
      log_summary(results, elapsed_time, csv_path)

if __name__ == "__main__":
      main()
