from threading import Thread
import sys
import time
import socket
import numpy as np
import maphosts as mh

class Produtor(Thread):
    def __init__(self,idprod,idfila,ciclosprod):
        super().__init__()
        self.idprodutor = idprod
        self.idfila = idfila
        self.ciclos = ciclosprod

    def run(self):
        # -------( Estabelece SOCKETs )
        socket_prod = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        end_fila = mh.endereco[self.idfila] # endereço (ip,porta) cadastrado em maphosts.py
        
        try:
            contador = 0
            # --- Loop de ciclos ---
            for ciclo,qtd_mensagens,intervalo in self.ciclos:
                # gerar pacote de mensagens do ciclo (usando Poisson)
                blocos_msgs = self.calcula_distribuicao(qtd_mensagens,intervalo)
                self.exibe(f'Iniciando ciclo {ciclo} -------')
                for bl in blocos_msgs:
                    for m in range(bl):
                        mensagem = f'A,*** msg/blk {m}/{bl} do ciclo {ciclo} ***'
                        # envia mensagem para a fila
                        socket_prod.sendto(mensagem.encode("utf-8"), end_fila)
                        # ------ aviso a cada 100 mensagens ------
                        contador +=1
                        if contador%100 == 0:
                            self.exibe(f'{contador} msgs enviadas')
                        # ----------------------------------------
                        # recebe resposta da fila
                        #resposta, end_resp = socket_prod.recvfrom(1024)
                        time.sleep(0.3) # atraso forçado para náo engarrafar as filas

        except Exception as erro:
            self.exibe(f'[ERRO] {erro}')
        finally:
            # avisa a fila que seu produtor vai encerrar
            mensagem = f'A,E,*** mensagem de encerramento ***'
            socket_prod.sendto(mensagem.encode("utf-8"), end_fila)
            # fecha o socket
            socket_prod.close()
            self.exibe(f'Produtor encerrado.')

    def calcula_distribuicao(self,num_blocos,tempo_aprox):
        # calculando a media de pacotes por segundo
        segundos = tempo_aprox * 60
        media_blocos = num_blocos / segundos
        # gerando distribuição de pacotes no tempo definido
        blocosCalculados = np.random.poisson(lam=media_blocos, size=segundos) 
        return blocosCalculados 

    def exibe(self,status):
        print(f'[PRODUTOR {self.idprodutor}] {status}')

# -----------------------------------( PRINCIPAL )
def gerador_cenario(cena,num_ciclos,num_msgs,interv_tempo):
    ciclos_fila1 = []
    ciclos_fila2 = []
    ciclos_fila3 = []
    for cl in range(num_ciclos):
        fator = np.linspace(1.0, 2.0, num=num_ciclos, endpoint=False)
        if (cena == 0): 
            # Cenário 0: NENHUMA fila sobrecarregada
            ciclos_fila1.append((cl+1,num_msgs,interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs,interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs,interv_tempo)) 
        elif (cena == 1): 
            # Cenário 1: UMA fila (f1) sobrecarregada, duas sem sobrecarga
            ciclos_fila1.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs,interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs,interv_tempo)) 
        elif (cena == 2): 
            # Cenário 2: DUAS filas (f1,f2) sobrecarregadas, uma sem sobrecarga
            ciclos_fila1.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs,interv_tempo)) 
        elif (cena == 3): 
            # Cenário 2: TRÊS filas (f1,f2,f3) sobrecarregadas
            ciclos_fila1.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
        elif (cena == 4): 
            # Cenário 4: OUTRAS DUAS filas (f1,f3) sobrecarregadas, uma sem sobrecarga
            ciclos_fila1.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs,interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
        elif (cena == 5): 
            # Cenário 4: OUTRAS DUAS filas (f2,f3) sobrecarregadas, uma sem sobrecarga
            ciclos_fila1.append((cl+1,num_msgs,interv_tempo)) 
            ciclos_fila2.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
            ciclos_fila3.append((cl+1,num_msgs*fator[cl],interv_tempo)) 
    return ciclos_fila1,ciclos_fila2,ciclos_fila3
        
# -------------------------------------------------------------------
print('''
+-----------------------------------------------------------+
| Módulo Produtor de Mensagens                              | 
| ============================                              |
| Este módulo alimenta filas de mensagens criando threads   |
| que produzem mensagens de tamanho fixo e as enviam para   |
| filas via socket.                                         |
| Cada thread produtora obedece as seguintes configurações: |
|   -> cenario: = 0 (há seis cenários possíveis [0 a 5], e  |
|                    cada um contempla um comportamento de  |
|                    sobrecarga de fila)                    |
|   -> qtdciclos = 3 (quantidade de ciclos de geração de    |
|                     pacotes de mensagens - cada cenário   |
|                     muda a quantidade de mensagens em     |
|                     cada ciclo)                           |
|   -> qtdblocos = 500 (quantidade inicial de mensagens em  |
|                       cada pacote por ciclo de mensagens  |
|                       de cada produtor.                   |
|                       cada cenário modifica este valor de |
|                       uma forma diferente)                |
|   -> qtdtempo = 5 (intervalo de tempo que serve de parâ-  |
|                    metro para calcular a distribuções de  |
|                    mensagens em um ciclo usando método de | 
|                    Poisson.                               |
|                    a informaçõe de tempo é em minutos)    |
+-----------------------------------------------------------+
| Para mudar estes valores, localize o trecho do código     |
| identificado como "Parâmetros de Controle da Execução"    |
| e faça as alterações desejadas                            |
+-----------------------------------------------------------+
| IMPORTANTE:                                               |
| Para o funcionamento correto do ambiente, assume-se que o |
| o arquivo "maphosts.py" foi adequadamente modificado com  |
| os endereços IPs corretos de cada componete da solução.   |
+-----------------------------------------------------------+
        ''')
# ---------------------------------- #
# Parâmetros de Controle da Execução #
# ---------------------------------- #
cenario = 0         # indica qual o cenário (comportamento de produção de msgs)
qtdciclos = 3       # Qtd de ciclos de pacotes de mensagens
qtdblocos = 500    # Qtd de blocos de mensagens por ciclo de envio para cada produtor
qtdtempo = 5        # tempo aproximado (em mim) para enviar cada pacote à fila
# ---------------------------------- #

# criação das threads produtor

print(f'[PRODUTOR] Iniciando Produtores...')
clf1,clf2,clf3 = gerador_cenario(cenario,qtdciclos,qtdblocos,qtdtempo)
prod1 = Produtor(idprod='Prod01',idfila='Fila01',ciclosprod=clf1)
prod2 = Produtor(idprod='Prod02',idfila='Fila02',ciclosprod=clf2)
prod3 = Produtor(idprod='Prod03',idfila='Fila03',ciclosprod=clf3)

input(f'[PRODUTOR] Tecle <ENTER> para continuar')

prod1.start()
prod2.start()
prod3.start()

prod1.join()
prod2.join()
prod3.join()

print('[PRODUTOR] Fim')