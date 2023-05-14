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

# Descobre ip de uma máquina
import socket 
sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sk.connect(('8.8.8.8', 53))
local = sk.getsockname()[0]
sk.close()

# Monta os enderecos
endereco = {
    'Fila01':(local,9001),
    'Fila02':(local,9002),
    'Fila03':(local,9003),
    'Agente':(local,9009),
}
