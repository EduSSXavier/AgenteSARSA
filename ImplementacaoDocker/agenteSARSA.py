# ------------------------------------------------------------
# agenteSARSA.py
# ==============
# Agente que regula a distribuição de largura de banda entre 
# as filas de mensagens utilizadas na prova de conceito.
# O agente utiliza o algoritmo SARSA para compor e adaptar uma
# matriz-Q com seu aprendizado e decidir ações de intervenção
# na largura de banda de cada fila de mensagem. Para tanto, 
# apoia-se em informações previamente estabelecidas em uma 
# matriz de recompensas.
# ------------------------------------------------------------

# ---[ Importação de bibliotecas ]---
import pandas as pd
import pathlib
import socket
import random
import maphosts as mh

# -------------------------------------------------------------------
# CLASSE: Estado
# -------------------------------------------------------------------
class Estado:
    def __init__(self,buffersInf):
        '''
        Um estado é um dicionário de dicionários representando 
        a situação dos buffers gerenciados em determinado instante.
        Composição:
            { 
                Id da fila/Buffer: { 
                    "max": Tamanho máximo do buffer,
                    "tam": Ocupação atual do buffer (qtd. de itens),
                    "tx" : Taxa de transmissão aplicada ao buffer
                    "lim : Limite (threshold) do buffer
                            }
            }
        '''
        self.buffers = buffersInf
        
    def calculaFaixa(self):
        faixa = ''
        for bf in self.buffers:
            # verifica tamanho do buffer contra o threshold estabelecido
            if (self.buffers[bf]['tam'] <= self.buffers[bf]['lim']): 
                faixa += 'V'
            else:
                faixa += 'C'
        return faixa

# -------------------------------------------------------------------
# CLASSE: Agente
# -------------------------------------------------------------------
class Agente:
    def __init__(self, taxaEpsilon, taxaAlfa, fatorGama, maxEpisodios):
        '''
        Um agente é quem avalia um estado e, aplicando o algoritmo SARSA, 
        decide qual o ajuste a ser feito nas taxas de transmissão de cada 
        buffer, a partir de uma matriz-Q onde evoliu seu aprendizado e de 
        uma matriz de recompensas com valores previamente definidos.
        '''
        print("[Agente SARSA] Agente instanciado.")

        # -----------------------------------------------------------
        #
        # Definições iniciais de valores da classe (inicializações)
        #

        # Parâmetro épsilon: Taxa de aleatoriedade
        #       - Seu valor é comparado a um valor randômico 'X' a cada avaliação
        #           - Se X < épsilon, a nova ação é apenas sorteada
        #           - Do contrário, procura-se a nova ação na matriz Q
        #       - O artigo (Joberto) sugere testar com 8%, 16%, 32% e 64%
        #       - Minha pesquisa pode simular diversas taxas e comparar resultados
        self.epsilon = taxaEpsilon

        # Parâmetro alfa: Taxa de aprendizado
        #       - Seu valor é definido entre 0 e 1. 
        #       - Se alfa = 0 isso indica que não haverá aprendizado.
        #       - O artigo (Joberto) sugere testar inicialmente com 20% 
        #       - Minha pesquisa pode simular diversas taxas e comparar resultados
        self.alfa = taxaAlfa

        # Parâmetro gama: Fator de desconto
        #       - Seu valor é definido entre 0 e 1. 
        #       - Se alfa = 0 isso indica que não haverá aprendizado.
        #       - O artigo (Joberto) sugere testar inicialmente com 80% 
        #       - Minha pesquisa pode simular diversas taxas e comparar resultados
        self.gama = fatorGama

        # Quantidade máxima de episódios que o agente executa a cada chamada (default=100)
        self.qtdMaxEpisódios = maxEpisodios

        # Estados e Ações possíveis
        # IMPORTANTE: Até o momento esta implementação prevê apenas 3 buffers e 3 ações 
        #             para cada buffer. Futuras implementações devem modificar este código
        #             para permitir a parametrização destas quantidades.
        #             Cada item das listas abaixo possui 3 posições, cada uma referente a 
        #             um dos 3 buffers controlados.

        # Estados possíveis = (3 buffers) ** (2 valores possíveis) = 8 itens
        #         "v" (Vazio): conteúdo do buffer igual ou inferior ao limite de ocupação
        #         "C" (Vazio): conteúdo do buffer superior ao limite de ocupação
        #
        #self.estadosPossiveis = ["VVV","VVC","VCV","VCC","CVV","CVC","CCV","CCC"]

        # Ações possíveis = (3 buffers) ** (3 valores possíveis) = 27 itens
        #         "+" (aumentar): taxa de transmissão do buffer deve aumentar em 10%
        #         "0" (manter)  : taxa de transmissão do buffer deve se manter onde está
        #         "+" (reduzir) : taxa de transmissão do buffer deve diminuir em 10%
        #
        #self.acoesPosssiveis  = ["+++","++-","++N","+-+","+--","+-N","+N+","+N-","+NN",
        #                         "-++","-+-","-+N","--+","---","--N","-N+","-N-","-NN",
        #                         "N++","N+-","N+N","N-+","N--","N-N","NN+","NN-","NNN"]

        # Matriz de Recompensas: 
        # IMPORTANTE: Seguindo as limitações de estados e ações possíveis (3 buffers),
        #             a matriz de recompensas também segue essa configuração.
        #             Futuras implementações devem modificar este código para permitir 
        #             a parametrização destas quantidades.
        #
        #
        self.matrizRecompensa = pd.read_csv('MatrizRecompensa_inicial.csv').set_index('Estados')

        # -----------------------------------------------------------
        # ===================
        # Alg.SARSA: Inicializa matriz Q (passo inicial do algoritmo)
        # ===================
        # A Matriz Q pode ser inicializada completamente zerada, caso não tenha havido 
        # nenhuma execução prévia do algoritmo, ou pode carregar a Matriz Q gerada por
        # uma execução anterior (continuando assim seu aprendizado). 
        # Para decidir seus valores iniciais, este módulo procura pela existência de um 
        # arquivo chamado "MatrizQ_atual.csv" (matriz contendo aprendizados anteriores 
        # registrados). Caso ele exista, carrega o mesmo na Matriz Q do programa. Caso 
        # contrário, os dados carregados são obtidos no arquivo "MatrizQ_inicial.csv".
        #
        # A matriz-Q armazena o aprendizado do algoritmo.
        # Matriz Q: cada posição é composta por (estado, ação) -> valor
        # IMPORTANTE: Seguindo as limitações de estados e ações possíveis (3 buffers),
        #             a matriz-Q também segue essa configuração.
        #             Futuras implementações devem modificar este código para permitir 
        #             a parametrização destas quantidades.
        #
        print("[Agente SARSA] Inicializando Matriz-Q.")

        # define que a Matriz Q vai começar zerada
        nomeArquivoMatrizQ = 'MatrizQ_inicial.csv'
        # procura se existe uma Matriz Q mais atual para começar (com aprendizados anteriores)
        arquivos = pathlib.Path('./').glob('MatrizQ_atual.csv')
        for arq in arquivos:
            nomeArquivoMatrizQ = arq
        # carrega Matriz Q a partir do arquivo escolhido
        self.matrizQ = pd.read_csv(nomeArquivoMatrizQ).set_index('Estados')
        
    def avaliaEstado(self,estadoInf,numEpDecorridos):
        '''
        Esta função testa se o processamento atingiu ponto de parada.
        Os pontos de parada são:
            (1) O processamento executou a qtd. máxima de episódios definidos
                na criação do agente
            (2) O processamento atingiu a situação ideal 
                (b1, b2 e b3 próximos do limite) e (tx1 > tx2 > tx3)
                Os limites são estabelecidos na criação dos buffers.
                A ordenação das taxas estabelece uma prioridade entre
                os buffers (do primeiro ao terceiro em ordem decrescente).
        '''
        #print('--->'*2,f'[SARSA] Entrou em avaliar estado')
        parar = False
        # Testes de condição de parada
        if (numEpDecorridos == self.qtdMaxEpisódios):
            # Teste (1) - verifica se atingiu limite de episódios
            parar = True # condição de parada (1) atingida
            print('[SARSA] Atingiu condição de parada 1 (limite de episódios).')
        else:
            # Teste (2) - verifica se atingiu a situação ideal
            buffersOK = 0
            listaTaxasInf = []
            for bf in estadoInf.buffers.values():
                if ((bf['tam'] <= (bf['lim'] * 1.1)) and 
                    (bf['tam'] >= (bf['lim'] * 0.9))):
                    buffersOK += 1
                listaTaxasInf.append(bf['tx'])
            if ((buffersOK == len(estadoInf.buffers)) and 
                (listaTaxasInf == sorted(listaTaxasInf,reverse=True))):
                parar = True # condição de parada (2) atingida
                print('[SARSA] Atingiu condição de parada 2 (situação ideal).')

        # retorna avaliação feita
        return parar


    def defineAcao(self,estadoInf):
        '''
        Esta função aplica política épsilon-greedy para decidir a próxima ação do agente.
        '''
        acaoResposta = ''

        #print(f'[SARSA] Entrou em define ação')

        # obtém um valor randômico e compara o mesmo com a taxa épsilon
        if (random.random() < self.epsilon):
            # sorteia uma ação (+/N/-) para cada buffer
            acaoResposta += random.choice(['+','N','-']) +   \
                            random.choice(['+','N','-']) +   \
                            random.choice(['+','N','-']) 
        else:
            # recupera a maior valor de ação para o estado informado
            acaoResposta = self.matrizQ.loc[estadoInf.calculaFaixa()].idxmax()
        # retorna ação escolhida
        return acaoResposta

    def aplicaAcao(self, estadoInf, acaoInf):
        '''
        Esta função aplica a ação definida peLo agente, alterando as taxas de transmissão.
        '''
        # cria uma cópia do estado atual
        novoEstado = Estado(estadoInf.buffers.copy())
        # calcula as novas taxas de transmissão
        somaTaxas = sum(bf['tx'] for bf in novoEstado.buffers.values())
        indAcao = 0
        for bf in novoEstado.buffers.values():
            if (acaoInf[indAcao] == '+'):
                # ação '+' aumenta taxa de transmissão em 10%
                tx = bf['tx']
                novoValorTx = (tx * 1.10)
                valMaximo = (1 + tx - somaTaxas) if (1 + tx - somaTaxas) > 0 else tx
                if (novoValorTx) <= valMaximo:
                    if (tx) < valMaximo:
                        bf['tx'] = novoValorTx
            elif (acaoInf[indAcao] == '-'):
                # ação '-' reduz taxa de transmissão em 10%
                tx = bf['tx']
                bf['tx'] = (tx * 0.90) if (tx * 0.90) >= 0.10 else 0.10
            #else:
                # ação 'N' não altera a taxa de transmissão
                #pass
            indAcao += 1
        return novoEstado

    def calculaRecompensa(self, estadoInf, acaoInf):
        '''
        Esta função calcula a recompensa referente a uma ação aplicada em um estado
        '''
        # calcula faixa do estado (ex: "VVC", "CVC", "VCV", ...)
        faixaEstado = estadoInf.calculaFaixa()
        # recupera e retorna recompensa da matriz recompensa(faixa,ação)
        recompensa = self.matrizRecompensa.loc[faixaEstado][acaoInf]
        return recompensa


    def processarEstado(self, estadoInf):
        '''
        Esta executa os passos do alg. SARSA para um estado informado.
        (exceto o primeiro passo, que é executado no método construtor da classe Agente)
        '''
        # Inicializa o contador de episódios decorridos
        numEp = 1  

        # -----------------------------------------------------------
        # ===================
        # Alg.SARSA: Define próxima ação
        # ===================
        #
        acaoAtual = self.defineAcao(estadoInf)
        #print(f'[SARSA] Definiu ação atual')

        # -----------------------------------------------------------
        # ===================
        # Alg.SARSA: Loop enquanto não atingir condições de parada
        # ===================
        novoEstado = estadoInf
        while (True):
            print('---> [SARSA] episódio:',numEp,'-'*40)
            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Avalia estado -  É hora de parar?
            # =====================
            if (self.avaliaEstado(estadoInf,numEp)):
                break

            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Aplica nova ação
            # =====================
            novoEstado = self.aplicaAcao(estadoInf,acaoAtual)

            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Calcula recompensa
            # =====================
            recompensa = self.calculaRecompensa(estadoInf,acaoAtual)

            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Define próxima ação
            # =====================
            novaAcao = self.defineAcao(novoEstado)

            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Atualiza matriz Q
            # =====================
            faixaEstadoAtual = estadoInf.calculaFaixa()
            novaFaixaEstado = novoEstado.calculaFaixa()
            valorAtualQ = self.matrizQ[acaoAtual][faixaEstadoAtual]
            valTemp = recompensa + (self.gama * self.matrizQ[novaAcao][novaFaixaEstado])
            novoValorQ = valorAtualQ + (self.alfa * (valTemp - valorAtualQ))
            self.matrizQ.at[faixaEstadoAtual,acaoAtual] = novoValorQ

            # -----------------------------------------------------------
            # =====================
            # Alg.SARSA: Atualiza numero de episódios, estado e ação 
            #                        (com novo estado e nova ação)
            # ====================
            numEp +=1
            estadoInf = novoEstado
            acaoAtual = novaAcao

        # ------------(fim do loop)

        # -----------------------------------------------------------
        # ===================
        # Alg.SARSA: Retorna resultado final (estadoInf modificado pelo SARSA)
        # ===================
        return novoEstado
        # ==================
    
    def salvarMatrizQ(self):
        '''
        Esta função salva um arquivo em formato "CSV" contendo a nova Matriz-Q
        que foi gerada a partir da original, mas tem o aprendizado acumulado
        nas vezes em que o algoritmo SARSA executou.
        OBS: O salvamento não guarda histórico. Ele sempre sobrepõe a matriz
        anterior com a mais atual (apenas a original, totalmente zerada, não 
        segue essa regra)
        '''
        self.matrizQ.to_csv("MatrixQ_atual.csv")


# -------------------------------------------------------------------
# Função local de execução do Agente SARSA
# -------------------------------------------------------------------
def executaSARSA():
    '''
    # -----------------------------------------------------------
    # Esta função inicializa o socket (servidor SARSA) e
    # instancia o agente SARSA.
    # Em seguida, executa um loop que aguarda chamadas das filas.
    #   - Cada chamada ao agente traz um estado (situação 
    #     atual das filas/buffers).
    #   - Cada resposta do SARSA devolve um novo estado (novas 
    #     taxas de trasmissão para cada fila/buffer)
    '''



    
    print('[SARSA] INÍCIO do PROCESSAMENTO do AGENTE SARSA')
    
    # -----------------------------------------------------------
    # Definição do arquivo de log
    # -----------------------------------------------------------
    #logSARSA = LogEventos('LogSARSA')
    #logSARSA.registrar('INICIO',0,logSARSA.nomeArquivo,'Início do log.')

    # -----------------------------------------------------------
    # Socket SARSA (atende solicitações de ajuste)
    # -----------------------------------------------------------
    # Cria o socket (servidor) para conversar com o agregador
    socketSARSA = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Define endereço do agente (ip,porta) cadastrado em maphosts.py 
    enderecoSARSA = mh.endereco['Agente']
    # Prepara o servidor para escutar mensagens
    socketSARSA.bind(enderecoSARSA)

    # -----------------------------------------------------------
    # Instanciando o agente SARSA
    # -----------------------------------------------------------
    agenteSARSA = Agente(taxaEpsilon = 0.08, taxaAlfa = 0.20, fatorGama = 0.80,
                         maxEpisodios = 500)
    # -----------------------------------------------------------
    # Loop de escuta/reação do agente
    # -----------------------------------------------------------
    contador = 0
    while (True):
        contador += 1
        print(f'[SARSA] Ciclo {contador} ------------------------------')
        # -----------------------------------------------------------
        # Recebendo solicitação (recebe estado atual de filas/buffers)
        # -----------------------------------------------------------
        # "estadoAtual" >>> é uma lista de dicionários composta de informações
        # de cada fila/buffer (id, tamanho maximo, tamanho atual, taxa de 
        # transmissão e threshold).
        # "enderecoSol" >>> é o endereço do solicitante que enviou o estado atual.
        # -----------------------------------------------------------
        estadoAtual, enderecoSol = socketSARSA.recvfrom(1024)
        estadoAtual = estadoAtual.decode("utf-8")

        # -----------------------------------------------------------
        # Tratando dados recebidos
        # -----------------------------------------------------------
        # Alg. SARSA vai avaliar o estado atual e propor um novo estado
        # como resposta ao Orquestrador
        # -----------------------------------------------------------
        if (estadoAtual == "FIM"):
            # Solicitante está encerrando e enviou mensagem de encerramento 
            # ao Agente SARSA, que também vai encerrar.
            break
        else:
            estadoAtual = eval(estadoAtual) # transformando texto recebido
            # Instanciando um objeto "Estado" a partir de "estadoAtual",
            # que é só um dicionario de dicionários recebida via socket
            objEstadoAtual = Estado(estadoAtual)
            #logSARSA.registrar('SARSA',contador,estadoAtual,'Estado recebido')
            novoObjEstado = agenteSARSA.processarEstado(objEstadoAtual)

        # -----------------------------------------------------------
        # Enviando resposta ao solicitante (novo estado com taxas de transmissão de buffers atualizadas)
        # -----------------------------------------------------------
        novoEstadoTxt = str(novoObjEstado.buffers)
        print('[SARSA] Novo estado / novas taxas calculadas')
        print(novoEstadoTxt)
        #logSARSA.registrar('SARSA',contador,novoEstado,'--> novo estado devolvido')
        socketSARSA.sendto(novoEstadoTxt.encode("utf-8"),enderecoSol)

    # -----------------------------------------------------------
    # Fim do processamento
    # -----------------------------------------------------------
    #logSARSA.registrar('FIM',0,logSARSA.nomeArquivo,'Fim do log.')
    # gravando a nova matrz Q (para preservar o aprendizado)
    agenteSARSA.salvarMatrizQ()
    # mensagem de encerramento
    print('[SARSA] FIM do PROCESSAMENTO do AGENTE SARSA')

# ---------------------------------------------------------------------------------
if __name__ == "__main__":
    print('''
        +-----------------------------------------------------------+
        | Módulo Agente SARSA                                       | 
        | ===================                                       |
        | Este módulo é acionado por uma fila quando seu threshold  |
        | é atingido. A comunicação entre as filas e o agente SARSA |
        | é feita por meio de sockets.                              |
        | Os parâmetros de funcionamento do agente são:             |
        |	    -> taxaEpsilon = 0.08                               |
        |       -> taxaAlfa = 0.20                                  |
        |	    -> fatorGama = 0.80                                 |
        |	    -> maxEpisodios = 500                               |
        | Consulte documentação dobre o algoritmo SARSA para mais   |
        | detalhes sobre o funcionamento destes parâmetros.         | 
        +-----------------------------------------------------------+
        | Para mudar estes valores, localize o trecho do código     |
        | identificado como "Instanciando o agente SARSA" e faça as |
        | alterações desejadas                                      |
        +-----------------------------------------------------------+
        | IMPORTANTE:                                               |
        | Para o funcionamento correto do ambiente, assume-se que o |
        | o arquivo "maphosts.py" foi adequadamente modificado com  |
        | os endereços IPs corretos de cada componete da solução.   |
        +-----------------------------------------------------------+
        ''')
    executaSARSA()
else:
    print("[Agente SARSA] [ERRO] Este agente executa de forma independente. O ORQUESTRADOR deve chamá-lo via socket.")
