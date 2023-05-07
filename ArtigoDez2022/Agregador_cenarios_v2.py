# Simulador do Agregador
import threading
import time
import socket
import numpy as np 
from LogEventos import LogEventos

# Variáveis de trabalho globais (controle de threads produtor/consumidor)
threadsProd = {}
continuarThreads = True

# -------------------------------------------------------------------
# CLASSE: Buffer
# -------------------------------------------------------------------
class Buffer:
    def __init__(self,idBuf,tamBuf,limBuf,taxaBuf):
        self.id = idBuf
        self.tamanhoMaximo = tamBuf
        self.limite = limBuf
        self.taxaTransm = round(taxaBuf,2)
        # "conteudo" é uma lista criada vazia 
        # (dados gerados/consumidos vão povoar a lista)
        self.conteudo = []
    
    def insere(self,dado):
        if (len(self.conteudo) >= self.tamanhoMaximo):
            return False    # Buffer cheio (pacote perdido)
        else:
            self.conteudo.append(dado)  # Dado inserido no buffer com sucesso (produção)
            return True
            
    def consome(self):
        if (len(self.conteudo) > 0):
            self.conteudo.pop()  # Dado retirado no buffer com sucesso (consumo)
            return True
        else:
            return False    # Buffer vazio (sem dados para consumir)

    def estouroLimite(self): # verifica se o threshold do buffer foi atingido
        if len(self.conteudo) > (self.tamanhoMaximo*self.limite):
            return True
        else:
            return False

# -------------------------------------------------------------------
# CLASSE: Thread Produtor (alimenta os 3 buffers)
# -------------------------------------------------------------------
class Produtor(threading.Thread):
    def __init__ (self,id,buffer,qtdsMsgs,tempos):
        threading.Thread.__init__(self)
        self.id = id
        self.bufferAlvo = buffer
        self.qtdsMsgs = qtdsMsgs
        self.tempos = tempos
        self.logThread = LogEventos('LogTh'+id)

    def calcula_distribuicao(self,pacs,tmpMin):
        # calculando a media de pacotes por segundo
        segundos = tmpMin*60
        mediaPacs = pacs / segundos
        # gerando distribuição de pacotes no tempo definido
        pacsCalculados = np.random.poisson(lam=mediaPacs, size=segundos) 
        # tempo máximo que cada grupo de pacotes deve demorar
        tempoMaximo = segundos / len(pacsCalculados) 
        return pacsCalculados,tempoMaximo

    def run(self):
        # A variável global threadsProd é um dicionário indexado pelo id da thread que
        # é usada para determinar quando cada threads produtoras acaba seu povoamento 
        # dos buffers. 
        # As threads consumidoras param de funcionar quando esta variável contiver
        # o valor 'False' para seu respectivo produtor e seus buffers estiverem vazios.
        global threadsProd
        threadsProd[self.id] = True

        pacotesPerdidos = 0

        # grava início da execução thread no log
        self.logThread.registrar('THR','INI',self.id,'Thread Produtor iniciada') 

        # gerar quantidades de mensagens usando a distribuição de Poisson
        # calculo da distribuição de pacotes
        pac1, max1 = self.calcula_distribuicao(self.qtdsMsgs[0],self.tempos[0])
        pac2, max2 = self.calcula_distribuicao(self.qtdsMsgs[1],self.tempos[1])
        pac3, max3 = self.calcula_distribuicao(self.qtdsMsgs[2],self.tempos[2])
        pac4, max4 = self.calcula_distribuicao(self.qtdsMsgs[3],self.tempos[3])
        pac5, max5 = self.calcula_distribuicao(self.qtdsMsgs[4],self.tempos[4])
        pacsCalc  = [pac1,pac2,pac3,pac4,pac5]
        tempoMaxCalc = [max1,max2,max3,max4,max5]

        # inserindo pacotes (mensagens) no buffer
        iniProc = time.time()
        dados = f'Produtor {self.id} enviou dados'
        for i in range(len(pacsCalc)):
            # trata cada bloco de pacotes
            for blkPac in pacsCalc[i]:
                ini = time.time()
                for p in range(blkPac):
                    operacao = self.bufferAlvo.insere(dados)
                    if ( operacao == False):
                        # Thread produtora perdeu pacote (buffer cheio)
                        pacotesPerdidos += 1
                        #self.logThread.registrar('THR','BCH','b1','Buffer cheio (pacote perdido)')
                fim = time.time()
                sono = tempoMaxCalc[i] - (fim-ini)
                sono = sono if sono > 0 else 0
                time.sleep(sono)
        fimProc = time.time()
        self.logThread.registrar('THR','PROD',self.id,str((fimProc - iniProc)/60),'inseriu todas as mensagens na fila')
        
        # Informa que a thread Produtor encerrou
        threadsProd[self.id] = False
        self.logThread.registrar('THR','PKT',self.id,str(pacotesPerdidos),'pacotes perdidos')
        self.logThread.registrar('THR','FIM',self.id,'Thread Produtor encerrada') 

# -------------------------------------------------------------------
# CLASSE: Thread Consumidor (esvazia os 1 dos buffers)
# -------------------------------------------------------------------
class Consumidor(threading.Thread):
    def __init__ (self,id,buffer):
        threading.Thread.__init__(self)
        self.id = id
        self.bufferAlvo = buffer
        self.logThread = LogEventos('LogTh'+id)
    
    def run(self):
        # A variável global continuarThreads é "setada" externamente, 
        # determinando quando a thread deve parar.
        global threadsProd
        global continuarThreads

        bufferCheio = 0

        # grava início da execução thread no log
        self.logThread.registrar('THR','INI',self.id,'Thread consumidor iniciada',self.bufferAlvo.id) 


        # Loop de funcionamento da thread
        while (continuarThreads):
            # Thread do tipo CONSUMIDOR (vai esvaziar o buffer)
            operacao = self.bufferAlvo.consome()
            if ( operacao == False):
                # Thread consumidora encontrou buffer vazio
                bufferCheio += 1
                #self.logThread.registrar('THR','BVZ',self.id,self.bufferAlvo.id,'Buffer vazio')

            # Thread vai "dormir"
            # neste simulador, o tempo de parada é relativo a taxa de transmissão 
            # definida para o buffer:
            # - quanto maior a taxa, menos a thread dorme, logo, consome mais rápido
            # - quanto menor a taxa, mais a thread dorme, logo, consome mais devagar
            novaTaxa = 1 - (self.bufferAlvo.taxaTransm*3)
            if (novaTaxa > 0):
                tempoDormir = novaTaxa
            else:
                tempoDormir = 0
            #print('----->',self.id,self.bufferAlvo.id,self.bufferAlvo.taxaTransm,tempoDormir)

            # testar se a thread consumidora deve continuar funcionando
            time.sleep(tempoDormir)
            if (threadsProd[self.id] == False) and (len(self.bufferAlvo.conteudo) == 0):
                continuarThreads = False

        # Fim da thread
        self.logThread.registrar('THR','BVZ',self.id,self.bufferAlvo.id,str(bufferCheio),'Thread encontrou o buffer vazio')
        self.logThread.registrar('THR','FIM',self.id)

# -------------------------------------------------------------------
# CLASSE: Agregador
# -------------------------------------------------------------------
class Agregador:
    def __init__(self,listaBuffers):
        self.buffers = listaBuffers     # lista de buffers usados para distribuir dados 
                                        # (o artigo base usa apenas 3 buffers)
        self.produtores = []   #lista contendo threads produtoras (povoam buffers)
        self.consumidores = [] #lista contendo threads consumidoras (esvaziam buffers)

    def criarThreads(self,cenario):
        # Inicia threads:
        #       -> Uma thread de produção para cada buffer criado
        #       -> Uma thread de consumo para cada buffer criado
        #       -> Este trecho vai ser substituído futuramente por um gerador de pacotes
        '''
        The Slice Communication Evaluation Results
        The suite of tests includes overloading queues in the fol-
        lowing scenarios:
        • Scenario 1 - One of the queues is overloaded;
        • Scenario 2 - Two queues are overloaded; and
        • Scenario 3 - All queues are overloaded.
        The queues overload is configured for verifying the agent
        behavior as follows:
        • 30% above its defined limit for 10 minutes;
        • 50% above its defined limit for 10 minutes;
        • 80% above its defined limit for 10 minutes; and
        • 100% above its defined limit for 10 minutes
        '''

        # Criando threads produtoras
        baseMsgs = 10000 # pacotes/msgs que cada intervalo da distribuição vai lidar
        baseTempo = 10    # tempo (em mim) para enviar os pacotes ao buffer
        if (cenario==0): # No queues overloaded: b1,b2,b3
            qtdsMsgs = [baseMsgs,baseMsgs,baseMsgs,baseMsgs,baseMsgs]
            qtdsTempo = [baseTempo,baseTempo,baseTempo,baseTempo,baseTempo]
            self.produtores.append(Produtor(self.buffers[0].id,self.buffers[0],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[1].id,self.buffers[1],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[2].id,self.buffers[2],qtdsMsgs,qtdsTempo))
        elif (cenario==1): # One f the queues is overloaded: <b1>,b2,b3
            qtdsMsgs = [baseMsgs,baseMsgs*1.30,baseMsgs*1.50,baseMsgs*1.80,baseMsgs*2.00]
            qtdsTempo = [baseTempo,baseTempo,baseTempo,baseTempo,baseTempo]
            self.produtores.append(Produtor(self.buffers[0].id,self.buffers[0],qtdsMsgs,qtdsTempo))
            qtdsMsgs = [baseMsgs,baseMsgs,baseMsgs,baseMsgs,baseMsgs]
            self.produtores.append(Produtor(self.buffers[1].id,self.buffers[1],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[2].id,self.buffers[2],qtdsMsgs,qtdsTempo))
        elif (cenario==2): # Two queues are overloaded: <b1>,<b2>,b3
            qtdsMsgs = [baseMsgs,baseMsgs*1.30,baseMsgs*1.50,baseMsgs*1.80,baseMsgs*2.00]
            qtdsTempo = [baseTempo,baseTempo,baseTempo,baseTempo,baseTempo]
            self.produtores.append(Produtor(self.buffers[0].id,self.buffers[0],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[1].id,self.buffers[1],qtdsMsgs,qtdsTempo))
            qtdsMsgs = [baseMsgs,baseMsgs,baseMsgs,baseMsgs,baseMsgs]
            self.produtores.append(Produtor(self.buffers[2].id,self.buffers[2],qtdsMsgs,qtdsTempo))
        elif (cenario==3): # All queues are overloaded: <b1>,<b2>,<b3>
            qtdsMsgs = [baseMsgs,baseMsgs*1.30,baseMsgs*1.50,baseMsgs*1.80,baseMsgs*2.00]
            qtdsTempo = [baseTempo,baseTempo,baseTempo,baseTempo,baseTempo]
            self.produtores.append(Produtor(self.buffers[0].id,self.buffers[0],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[1].id,self.buffers[1],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[2].id,self.buffers[2],qtdsMsgs,qtdsTempo))
        else: # Another two queues are overloaded: b1,<b2>,<b3>
            qtdsMsgs = [baseMsgs,baseMsgs,baseMsgs,baseMsgs,baseMsgs]
            qtdsTempo = [baseTempo,baseTempo,baseTempo,baseTempo,baseTempo]
            self.produtores.append(Produtor(self.buffers[0].id,self.buffers[0],qtdsMsgs,qtdsTempo))
            qtdsMsgs = [baseMsgs,baseMsgs*1.30,baseMsgs*1.50,baseMsgs*1.80,baseMsgs*2.00]
            self.produtores.append(Produtor(self.buffers[1].id,self.buffers[1],qtdsMsgs,qtdsTempo))
            self.produtores.append(Produtor(self.buffers[2].id,self.buffers[2],qtdsMsgs,qtdsTempo))

        # Criando threads consumidoras
        for i in range(len(self.buffers)):
            self.consumidores.append(Consumidor(self.buffers[i].id,self.buffers[i]))
        
        # iniciando as threads
        for i in range(len(self.buffers)):
            self.produtores[i].start()
        for i in range(len(self.buffers)):
            self.consumidores[i].start()

    def finalizarThreads(self):
        global continuarThreads
        continuarThreads = False
        # iniciando as threads
        for i in range(len(self.buffers)):
            self.produtores[i].join()
        for i in range(len(self.buffers)):
            self.consumidores[i].join()

    def atualizarTaxas(self,taxas):
        #   Executa a alteração nas taxas de cada buffer
        # "taxas" deve ser uma lista de dicionários {idBuffer:valor, taxa:valor}
        for bf in self.buffers:
            bf.taxaTransm = round(taxas[bf.id],2)

    def verificaThresholds(self):
        # -----------------------------------------------------------
        # Verifica se os thresholds dos buffers foram atingidos.
        # -----------------------------------------------------------
        # - Cada buffer tem um limite (threshold) de preenchimento definido em sua criação
        # - Se o limite atingido, solicita uma ação ao orquestrador (SARSA)
        # -----------------------------------------------------------
        for bf in self.buffers:
            if bf.estouroLimite(): # threshold ultrapassado?
                return True
        return False


# -------------------------------------------------------------------
# Função local de execução do Agregador
# -------------------------------------------------------------------
def executaAgregador():
    # -----------------------------------------------------------
    # Esta função cria buffers que são alimentados e esvaziados
    # por threads produtoras e consumidoras.
    #
    # Embora o código seja de fácil adaptação, no momento está
    # trabalhando com apenas 3 buffers em sua simulação, visando
    # simplificar a programação do alg. SARSA, que utiliza matrizes
    # baseadas no número de buffers definidos (os dados dessas matrizes
    # por hora se baseiam em artigo fornecido pelo Prof. Joberto e seria
    # complicado extrapolar novos dados, principalmente na matriz de recompensa)
    #
    # Esta função trabalha com um socket:
    #       Porta 5000: servidor Orquestrador (atende ao Agregador)
    #
    # Funcionamento:
    # - Define um arquivo de log
    # - Define 3 buffers
    # - Instancia um Agregador que gerencia os 3 buffers
    # - - Cria threads produtoras e consumidoras
    # Após o socket ser devidamente definido, 
    # executa um loop que:
    #   - chama o Orquestrador
    #   - trata resposta alterando taxas de transmissão dos buffers
    # -----------------------------------------------------------
    print("[AGREGADOR] INÍCIO do PROCESSAMENTO.")
    '''
    The Slice Communication Evaluation Results
    The suite of tests includes overloading queues in the fol-
    lowing scenarios:
    • Scenario 1 - One of the queues is overloaded;
    • Scenario 2 - Two queues are overloaded; and
    • Scenario 3 - All queues are overloaded.
    The queues overload is configured for verifying the agent
    behavior as follows:
    • 30% above its defined limit for 10 minutes;
    • 50% above its defined limit for 10 minutes;
    • 80% above its defined limit for 10 minutes; and
    • 100% above its defined limit for 10 minutes
    '''
    cenario = int(input('Informe o cenário (0,1,2 ou 3 ou 4): '))

    # -----------------------------------------------------------
    # Definição do arquivo de log
    # -----------------------------------------------------------
    logAgregador = LogEventos('LogAgregador')
    logAgregador.registrar('INI',logAgregador.nomeArquivo)

    # -----------------------------------------------------------
    # Instancia Buffers (taxas tiradas do artigo - verificar valores depois)
    # -----------------------------------------------------------
    buffer1 = Buffer(idBuf='b1',tamBuf=1000,limBuf=0.50,taxaBuf=0.35)
    buffer2 = Buffer(idBuf='b2',tamBuf=1000,limBuf=0.50,taxaBuf=0.25)
    buffer3 = Buffer(idBuf='b3',tamBuf=1000,limBuf=0.50,taxaBuf=0.20)
    print("[AGREGADOR] Buffers criados")

    # -----------------------------------------------------------
    # Instancia Agregador
    # -----------------------------------------------------------
    agregador = Agregador([buffer1,buffer2,buffer3])
    print("[AGREGADOR] Agregador instanciado")

    # -----------------------------------------------------------
    # Cria threads produtor/consumidor
    # -----------------------------------------------------------
    agregador.criarThreads(cenario)
    print("[AGREGADOR] Threads criadas")

    # -----------------------------------------------------------
    # Cria um socket datagrama apontando para o Orquestrador
    # -----------------------------------------------------------
    socketOrq = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    portaOrq = 5000
    # Define o endereco do socket (Ip e porta)
    enderecoOrq = ('192.168.15.187', portaOrq)

    # -----------------------------------------------------------
    # Loop de chamada do Orquestrador
    # -----------------------------------------------------------
    contador = 1
    while (continuarThreads):
        print(f'[AGREGADOR] ({contador})---------------------------------')
        # Status dos 3 buffers
        info = ''
        for bf in agregador.buffers:
                info += '('+bf.id+','+str(len(bf.conteudo))+','+str(bf.taxaTransm)+');'
        info += 'Situação dos buffers'
        logAgregador.registrar('BUF','SIT',info)
        print('Situação dos buffers -> ',info)

        # -----------------------------------------------------------
        # Verifica se os thresholds dos buffers foram atingidos.
        # -----------------------------------------------------------
        # - Cada buffer tem um limite de preenchimento definido em sua criação
        # - Se o limite atingido, solicita uma ação ao orquestrador (SARSA)
        # -----------------------------------------------------------
        print('[AGREGADOR] Verificando Thresholds')
        if (agregador.verificaThresholds()):
            # -----------------------------------------------------------
            # Chama o Orquestrador (passa situação de buffers e taxas atuais)
            # -----------------------------------------------------------
            estadoAtual = []
            info = ''
            for bf in agregador.buffers:
                # Montando a mensagem do estado 
                # (lista de dicionários com dados de cada buffer)
                dicBuf = {"id":bf.id, "max":bf.tamanhoMaximo, "tam":len(bf.conteudo), 
                                "tx":bf.taxaTransm, "lim":bf.limite}
                estadoAtual.append(dicBuf)
                info += bf.id+','+str(len(bf.conteudo))+','+str(bf.taxaTransm)+','
            # enviando estado para o orquestrador
            info += 'Threshold atingido'
            estadoAtual = str(estadoAtual) # ----> mensagem deve ser uma string
            print(f'[AGREGADOR] Chamando orquestrador\nEstado:{estadoAtual}')
            logAgregador.registrar('BUF','LIM',info)
            estadoAtual = estadoAtual.encode("utf-8")
            socketOrq.sendto(estadoAtual, enderecoOrq)
            # -----------------------------------------------------------
            # Trata resposta do Orquestrador (novas taxas de transmissão)
            # -----------------------------------------------------------
            # "novasTaxas" é um dicionário contendo as taxas calculadas pelo 
            # Orquestrador para cada buffer (buffer.id é a chave)
            # -----------------------------------------------------------
            novasTaxas, enderecoOrq = socketOrq.recvfrom(5000)
            novasTaxas = novasTaxas.decode("utf-8")
            print(f'[AGREGADOR] Recebeu resposta do orquestrador\nEstado:{novasTaxas}')
            novasTaxas = eval(novasTaxas) # transformando texto recebido em dicionário
            # Modificando as taxas nos buffers
            agregador.atualizarTaxas(novasTaxas)
            info = ''
            for bf in agregador.buffers:
                info += bf.id+','+str(bf.taxaTransm)+','
            info += 'Novas Taxas'
            logAgregador.registrar('BUF','NTX',info)

        # Dormindo por X segundos antes de executar o próximo ciclo
        # OBS: este tempo pode ser mudado para testar outros comportamentos
        time.sleep(0.2)
        contador += 1

    # -----------------------------------------------------------
    # Fim do processamento
    # -----------------------------------------------------------
    agregador.finalizarThreads()
    # Envio de mensagem de encerramento ao Orquestrador (para ele encerrar também)
    mensagem = "FIM".encode("utf-8")
    socketOrq.sendto(mensagem, enderecoOrq)
    # Fecha o arquivo de log
    logAgregador.registrar('FIM',logAgregador.nomeArquivo)
    print("[AGREGADOR] FIM do PROCESSAMENTO do AGREGADOR")


# ---------------------------------------------------------------------------------
if __name__ == "__main__":
    executaAgregador()
else:
    print("[AGREGADOR] [ERRO] Este módulo executa de forma independente.")
