import psycopg2
import redis
import json
import os
from time import sleep
from random import randint

class Worker:
    def __init__(self):
        redis_host = os.getenv('REDIS_HOST', 'queue')
        db_host = os.getenv('DB_HOST', 'db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_name = os.getenv('DB_NAME', 'sender')

        self.fila = redis.Redis(host=redis_host, port=6379, db=0)
        dns = f'dbname={db_name} user={db_user} host={db_host}'
        self.conn = psycopg2.connect(dns)

    def get_message(self):
        mensagem = json.loads(self.fila.blpop('sender')[1])
        print('Detectada nova mensagem:',mensagem['assunto'],', enviando...')
        self.send_message(mensagem)

    def send_message(self, mensagem):
        sleep(randint(15,45))
        print('Mensagem', mensagem['assunto'], ' enviada com sucesso!')
        self.update_database(mensagem['id'],'enviado')

    def update_database(self, id, status):
        SQL = 'UPDATE emails SET status = %s WHERE id = %s'
        cur = self.conn.cursor()
        cur.execute(SQL, (status, id))
        self.conn.commit()
        cur.close()
        print('Status atualizado!')

if __name__ == '__main__':
    print('Aguardando mensagens!')
    worker = Worker()
    while True:
        worker.get_message()

