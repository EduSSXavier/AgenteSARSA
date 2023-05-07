# Simulador do Orquestrador
import socket
from LogEventos import LogEventos

# -------------------------------------------------------------------
# Função local de execução do Orquestrador
# -------------------------------------------------------------------
def executaOrquestrador():
    # -----------------------------------------------------------
    # Esta função trabalha com dois sockets:
    #       (1) Porta 6000: cliente SARSA (solicita resposta do alg. SARSA)
    #       (2) Porta 5000: servidor Orquestrador (atende ao Agregador)
    # Após os sockets serem devidamente definidos, 
    # executa um loop que:
    #   - aguarda chamadas do Agregador
    #   - chama o alg. SARSA
    #   - devolve resposta do SARSA ao Agregador
    # -----------------------------------------------------------
    print("[ORQUESTRADOR] INÍCIO do PROCESSAMENTO do ORQUESTRADOR")

    # -----------------------------------------------------------
    # Definição do arquivo de log
    # -----------------------------------------------------------
    #logOrquestrador = LogEventos('LogOrquestrador')
    #logOrquestrador.registrar('INI',logOrquestrador.nomeArquivo,'Início do log.')

    # -----------------------------------------------------------
    # Socket cliente (SARSA atende Orquestrador)
    # -----------------------------------------------------------
    # Cria o socket apontando para o agente SARSA
    socketSARSA = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    portaSARSA = 6000
    # Define o endereco do socket (Ip e porta)
    enderecoSARSA = ('192.168.15.187', portaSARSA)

    # -----------------------------------------------------------
    # Socket servidor (Orquestrador atende Agregador))
    # -----------------------------------------------------------
    # Cria o socket (servidor) para conversar com o agregador
    socketOrq = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Define o endereco do socket (Ip e porta)
    portaOrq = 5000
    enderecoOrq = ('192.168.15.187', portaOrq)
    # Prepara o servidor para escutar mensagens
    socketOrq.bind(enderecoOrq)

    # -----------------------------------------------------------
    #
    # Loop de escuta/reação do agente
    #
    contador = 0
    while (True):
        contador += 1
        print(f'[ORQUESTRADOR] Ciclo {contador} ------------------------------')
        # -----------------------------------------------------------
        # Recebendo solicitação do Agregador (recebe estado atual de buffers)
        # -----------------------------------------------------------
        # "estadoAtual" é uma lista composta de informações da cada buffer 
        # (ou seja: id, tamanho maximo, tamanho atual, taxa de transmissão)
        # ou contém a string "FIM" para encerrar atividade do Orquestrador.
        # "enderecoAgreg" é o endereço do Agregador que enviou o estado atual.
        # -----------------------------------------------------------
        estadoAtual, enderecoAgreg = socketOrq.recvfrom(portaOrq)
        estadoAtual = estadoAtual.decode("utf-8")
        #logOrquestrador.registrar('ORQ','Recebeu chamado do agregador',str(contador))
        print(f'[ORQUESTRADOR] Recebeu chamado do agregador\nEstado Atual: {estadoAtual}')
        
        # -----------------------------------------------------------
        # Tratando dados recebidos
        # -----------------------------------------------------------
        if (estadoAtual == "FIM"):
            # Agregador está encerrando e enviou mensagem de encerramento aos Orquestrador
            break
        else:
            # OBS:
            # No estágio atual, o Orquestrador não realiza nenhuma tratamento nos 
            # dados enviados pelo Agregador. Ele simplesmente reenvia esses dados 
            # para o alg. SARSA.
            estadoAtual = estadoAtual.encode("utf-8")
            socketSARSA.sendto(estadoAtual,enderecoSARSA)
            # Recebe resposta do alg. SARSA (um novo estado)
            novasTaxas, endereco = socketSARSA.recvfrom(portaSARSA)

        # -----------------------------------------------------------
        # Enviando resposta ao Agregador (novo estado de buffers a ser aplicado)
        # -----------------------------------------------------------
        #logOrquestrador.registrar('ORQ','Enviando resposta ao Agregador')
        socketOrq.sendto(novasTaxas, enderecoAgreg)
        novasTaxas = novasTaxas.decode("utf-8")
        print(f'[ORQUESTRADOR] Respondeu ao agregador\nNovas Taxas de Transmissão: {novasTaxas}')

    # -----------------------------------------------------------
    # Fim do processamento
    # -----------------------------------------------------------
    # Envio de mensagem de encerramento ao agente SARSA (para ele encerrar também)
    # -----------------------------------------------------------
    mensagem = "FIM".encode("utf-8")
    socketSARSA.sendto(mensagem, enderecoSARSA)
    print("[ORQUESTRADOR] FIM do PROCESSAMENTO do ORQUESTRADOR")

# ---------------------------------------------------------------------------------
if __name__ == "__main__":
    executaOrquestrador()
else:
    print("[ORQUESTRADOR] [ERRO] Este módulo executa de forma independente. O AGREGADOR deve chamá-lo via socket.")
