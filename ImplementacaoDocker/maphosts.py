# ------------------------
# maphosts.py
# ------------------------
# Mapeamento dos endereços reais na rede para uso do
# experimento simulador.
# ------------------------
# Este arquivo é importado no início de cada programa.
# Caso os programas estejam executando em máquinas
# fisicamente separadas na rede, os endereços devem 
# ser revisados e uma cópia deste arquivo deve ser 
# incluída em cada imagem ou máquina virtual.
# ------------------------
endereco = {
    'Fila01':('192.168.15.103',9001),
    'Fila02':('192.168.15.103',9002),
    'Fila03':('192.168.15.103',9003),
    'Agente':('192.168.15.103',9009),
}

# ------------------------
# Rotina que descobre ip real de uma máquina
# ------------------------
#import socket 
#def obtem_ip_local():
#    sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#    sk.connect(('8.8.8.8', 53))
#    local = sk.getsockname()[0]
#    sk.close()
#    return local
