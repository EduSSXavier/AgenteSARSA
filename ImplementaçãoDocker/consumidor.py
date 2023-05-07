from threading import Thread
import sys
import socket
from time import sleep
import maphosts as mh

class Consumidor(Thread):
    def __init__(self,idcon,idfila):
        super().__init__()
        self.idconsumidor = idcon
        self.idfila = idfila

    def run(self):
        # -------( Estabelece SOCKET )
        socket_cons = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        end_fila = mh.endereco[self.idfila] # endereço (ip,porta) cadastrado em maphosts.py 
        try:
            contador = 0
            while True:
                # envia mensagem solicitando pacote/mensagem
                mensagem = 'C'
                socket_cons.sendto(mensagem.encode("utf-8"), end_fila)
                # Recebe pacote/mensagem retirada da fila
                msg, end_fila = socket_cons.recvfrom(1024)
                msg = msg.decode("utf-8")
                # ------ aviso a cada 100 mensagens ------
                contador +=1
                if contador%100 == 0:
                    self.exibe(f'{contador} msgs recebidas')
                # ---------------------------------------
                #self.exibe(f'Resposta {contador} da fila: {msg}')
                if msg[:1] == 'E':
                    # consumidor leu mensagem de encerramento (vai parar)
                    self.exibe(f'{msg}')
                    # consumidor avisa a fila
                    msg = 'E,*** mensagem de encerramento ***'
                    socket_cons.sendto(msg.encode("utf-8"),end_fila) # avisa a fila
                    break
        except Exception as erro:
            self.exibe(f'[ERRO] {erro}')
        finally:
            socket_cons.close()
            self.exibe(f'Encerrado.')

    def exibe(self,status):
        print(f'[CONSUMIDOR {self.idconsumidor}] {status}')

# -----------------------------------( PRINCIPAL )
print('''
+-----------------------------------------------------------+
| Módulo Consumidor de Mensagens                            | 
| ==============================                            |
| Este módulo consome mensagens das filas criando threads   |
| que usam sockets para solicitar mensagens.                |
| Cada thread consumidora precisa informa um nome (id) para |
| si e indica o nome (id) da fila que vai consumir.         |
+-----------------------------------------------------------+
| Foi criado um consumidor para cada fila.                  |
| Para acrescentar mais consumidores para uma unica fila    |
| é preciso definir novas threads no trecho de código que é |
| identificado como "Threads de Consumo".                   |
+-----------------------------------------------------------+
| IMPORTANTE:                                               |
| Para o funcionamento correto do ambiente, assume-se que o |
| o arquivo "maphosts.py" foi adequadamente modificado com  |
| os endereços IPs corretos de cada componete da solução.   |
+-----------------------------------------------------------+
        ''')
# ------------------ #
# Threads de Consumo #
# ------------------ #
prod1 = Consumidor(idcon='Cons01',idfila='Fila01')
prod2 = Consumidor(idcon='Cons02',idfila='Fila02')
prod3 = Consumidor(idcon='Cons03',idfila='Fila03')

input(f'[CONSUMIDOR] tecle <ENTER> para continuar')

print(f'[CONSUMIDOR] Iniciando consumidores...')

prod1.start()
prod2.start()
prod3.start()

prod1.join()
prod2.join()
prod3.join()

print(f'[CONSUMIDOR] Fim')