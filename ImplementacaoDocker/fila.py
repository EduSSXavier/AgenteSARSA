# ------------------------------------------------------------
# fila.py
# =======
# Gerenciador das 3 filas de mensagens utilizadas 
# na prova de conceito
# ------------------------------------------------------------

# ---[ Importação de bibliotecas ]---
import threading as th
from time import sleep
import socket
import maphosts as mh

# ---[ Variáveis globais ]---
estadoGlobal = {}
finalizamonitor = False

# ---[ classes ]---
class Fila(th.Thread):
    def __init__(self,idfila,tamanho,banda): #,limite):
        super().__init__()
        self.idfila = idfila
        #self.theshold = limite
        self.banda= banda/100
        self.tamanho = tamanho
        self.mensagens = []
    
    def run(self):
        # -------( Estabelece SOCKET )
        socket_fila = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        end_fila = mh.endereco[self.idfila] # endereço (ip,porta) cadastrado em maphosts.py 
        socket_fila.bind(end_fila)
        try:
            contador_prod = 0
            contador_cons = 0
            while True:
                # ---------- Escuta (aguarda mensagens)
                mensagem,end_remetente = socket_fila.recvfrom(1024)
                mensagem = mensagem.decode("utf-8")
                if mensagem[:1].upper() == 'A': # produtor alimenta fila
                    msg = mensagem[2:]
                    resposta = self.alimentar(msg)
                    socket_fila.sendto(resposta.encode("utf-8"),end_remetente)
                    # ------ aviso a cada 100 mensagens ------
                    contador_prod +=1
                    if contador_prod%100 == 0:
                        self.exibe(f'{contador_prod} msgs recebidas (produtor)')
                    # ----------------------------------------
                elif mensagem[:1].upper() == 'C': # consumidor lê fila
                    sitfila,resposta = self.consumir()
                    if sitfila == '0':
                        # msg recuperada (fila não estava vazia)
                        socket_fila.sendto(resposta.encode("utf-8"),end_remetente)
                elif mensagem[:1].upper() == 'E': # consumidor avisa que vai encerrar
                        # a fila deve encerrar também
                        break
                else:
                    self.exibe('Recebeu mensagem inválida (ignorou)')
                    continue
                self.dormir() # dorme para simular controle de banda da fila
        except Exception as erro:
            self.exibe(f'[ERRO] {erro}')
        finally:
            socket_fila.close()
            self.exibe(f'Encerrada.')
    
    def alimentar(self,msg):
        if len(self.mensagens) == self.tamanho:
            self.exibe(f'Fila cheia')
            resposta =  '1' # fila está cheia (mensagem foi perdida)
        else:
            self.mensagens.append(msg)
            # inseriu mensagem na fila com sucesso
            resposta = '0' 
        # atualiza estado global das filas
        self.atualiza_preenchimento_fila()
        return resposta

    def consumir(self):
        if len(self.mensagens) == 0:
            self.exibe(f'Fila vazia')
            ms = ' '
            sit = '1' # fila está vazia (não recupera nenhuma msg)
        else:
            ms = self.mensagens.pop(0)
            sit = '0'  # extraiu mensagem da fila
        # atualiza estado global das filas
        self.atualiza_preenchimento_fila()
        return sit,ms
        
    def dormir(self):
        # ---[ Lógica de "sono" ]--------------------------
        # Cria um delay "adormecendo" a thread para simular variação na 
        # largura de banda. O tempo de parada é calculado a partir de uma 
        # largura de banda definida para a fila em sua criação e depois 
        # ajustada pelo agente SARSA.
        # - quanto maior a banda, menos a thread dorme, logo, consome mais rápido
        # - quanto menor a banda, mais a thread dorme, logo, consome mais devagar

        # atualiza banda da fila
        global estadoGlobal
        self.banda = estadoGlobal[self.idfila]['tx']
        # calcula tempo de dormir
        novaBanda = 1 - self.banda
        if (novaBanda > 0):
            tempoDormir = novaBanda
        else:
            tempoDormir = 0
        sleep(tempoDormir)

    def atualiza_preenchimento_fila(self):
        # atualiza estado global da fila
        global estadoGlobal
        estadoGlobal[self.idfila]['tam'] = len(self.mensagens)

    def exibe(self,status):
        print(f'[FILA {self.idfila}] {status}')

class Monitor(th.Thread):
    def __init__(self,tmax_filas,th_filas,ban_filas):
        super().__init__()
        #self.bloqueio = th.Event()
        # inicializa estados globais das filas
        global estadoGlobal
        for fila in tmax_filas:
            estadoGlobal[fila] = {      # id da fila
                'max':tmax_filas[fila], # tamanho m-aximo da fila
                'tam':0,                # qtd mensagens (ocupação) na fila
                'tx':ban_filas[fila],   # percentual de banda da fila
                'lim':th_filas[fila],   # limite(threshold) definido para a fila
                                }
    def run(self):
        global estadoGlobal
        while True:
            if finalizamonitor:
                break
            # verifica thresholds
            # se houve violação de limite, acionar agente SARSA
            # e atualizar taxas de transmissão da filas
            chamaAgente = False
            for fila in estadoGlobal:
                if estadoGlobal[fila]['tam'] >= estadoGlobal[fila]['lim']:
                    self.exibe(f'Threshold atingido. Invocando Agente SARSA.')
                    chamaAgente = True
            if chamaAgente:
                # -------( Estabelece SOCKET do agente SARSA)
                socket_SARSA = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                # endereço (ip,porta) cadastrado em maphosts.py 
                end_SARSA = mh.endereco['Agente'] 
                # -------( Envia mensgem com o estado atual)
                estadoAtualTxt = str(estadoGlobal).encode("utf-8")
                socket_SARSA.sendto(estadoAtualTxt,end_SARSA)
                # Recebe resposta do alg. SARSA (um novo estado)
                novoEstadoTxt, end_SARSA = socket_SARSA.recvfrom(1024)
                # atualiza estado global
                novoEstado = eval(novoEstadoTxt.decode("utf-8"))
                for fila,dados in novoEstado:
                    estadoGlobal[fila]['tx'] = dados['tx']
                self.exibe('Taxas de transmissão da filas foram alteradas')
                '''
                evento_banda.set()
                '''
            sleep(0.5)

    def exibe(self,status):
        print(f'[MONITOR] {status}')



# -------------------------------------------------------------------
# Função local de execução das filas/buffers
# -------------------------------------------------------------------
print('''
    +-----------------------------------------------------------+
    | Módulo de Filas de Mensagens                              |
    | ============================                              |
    | Este módulo cria 3 threads de fila de mensagens.          |
    | Cada fila obedece as seguintes configurações:             |
    |   -> Tamanho da fila : 100 mensagens (todas as filas)     |
    |   -> Threshold da fila : 50 mensagens (todas as filas)    |
    |   -> Largura de banda simulada de cada fila:              |
    |   	-> Fila01 75% da banda                              |
    |   	-> Fila02 50% da banda                              |
    |   	-> Fila03 25% da banda                              |
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
tamanhomax_filas =  {'Fila01':100,'Fila02':100,'Fila03':100} #eval(tamanho)
threshold_filas =  {'Fila01':50,'Fila02':50,'Fila03':50} #eval(limite)
banda_filas = {'Fila01':75,'Fila02':50,'Fila03':25} #eval(perc_banda)
# ---------------------------------- #

# criação e acionamento das threads de fila e do monitor
print(f'[FILAS] Iniciando Monitor...')

monitor = Monitor(tamanhomax_filas,threshold_filas,banda_filas)

print(f'[FILAS] Iniciando Filas...')

fila1 = Fila('Fila01',tamanhomax_filas['Fila01'],banda_filas['Fila01'])
fila2 = Fila('Fila02',tamanhomax_filas['Fila02'],banda_filas['Fila02'])
fila3 = Fila('Fila03',tamanhomax_filas['Fila03'],banda_filas['Fila03'])

input(f'[FILAS] tecle <ENTER> para continuar')

monitor.start()
fila1.start()
fila2.start()
fila3.start()

fila1.join()
fila2.join()
fila3.join()
finalizamonitor = True
monitor.join()


print('[FILAS] Fim')
