import os
import re
import tempfile
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF

# ==========================================
# CONFIGURAÇÕES
# ==========================================
PASTA_OCR_LOCAL = "textos_ocr"

def normalizar_texto(texto):
    if not texto: return ""
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def normalizar_cpf(cpf):
    if not cpf: return ""
    return re.sub(r'\D', '', cpf)

# ==========================================
# BUSCA LOCAL (TXTs)
# ==========================================
def buscar_local_txt(nome, ano=None, mes=None, dia=None, cpf=None):
    nome_norm = normalizar_texto(nome)
    cpf_norm = normalizar_cpf(cpf)
    resultados = []

    if not os.path.exists(PASTA_OCR_LOCAL):
        return resultados

    # Cria o padrão de busca (Ex: "1980.03.15" ou "1980.03" ou "1980")
    partes_prefixo = []
    if ano: partes_prefixo.append(str(ano))
    if mes: partes_prefixo.append(str(mes).zfill(2))
    if dia: partes_prefixo.append(str(dia).zfill(2))
    
    prefixo_busca = ".".join(partes_prefixo)

    arquivos_alvo = [f for f in os.listdir(PASTA_OCR_LOCAL) 
                     if f.startswith(prefixo_busca) and f.endswith('_ocr.txt')]
    
    for arquivo in arquivos_alvo:
        caminho_txt = os.path.join(PASTA_OCR_LOCAL, arquivo)
        with open(caminho_txt, 'r', encoding='utf-8') as f:
            conteudo = f.read()
            
        # Divide o texto com base no padrão --- PÁGINA X --- (ignorando maiúsculas/minúsculas e acentos soltos)
        # partes[0] será o texto antes da primeira página, partes[1] o número da pág, partes[2] o texto da pág...
        partes = re.split(r'---\s*P[AÁ]GINA\s+(\d+)\s*---', conteudo, flags=re.IGNORECASE)
        
        paginas_encontradas = []

        if len(partes) == 1:
            # Caso o arquivo não tenha a marcação de página por algum motivo
            conteudo_norm = normalizar_texto(partes[0])
            if nome_norm in conteudo_norm and (not cpf_norm or cpf_norm in conteudo_norm):
                paginas_encontradas.append("Desconhecida")
        else:
            # Pula de 2 em 2 pegando o número da página e o texto correspondente
            for i in range(1, len(partes), 2):
                num_pagina = partes[i]
                texto_pagina = partes[i+1]
                
                conteudo_norm = normalizar_texto(texto_pagina)
                
                achou_nome = nome_norm in conteudo_norm
                achou_cpf = True if not cpf_norm else (cpf_norm in conteudo_norm)
                
                if achou_nome and achou_cpf:
                    paginas_encontradas.append(num_pagina)
                    
        if paginas_encontradas:
            p = arquivo.split('.')
            data_fmt = f"{p[2]}/{p[1]}/{p[0]}"
            paginas_str = ", ".join(paginas_encontradas)
            print(f"  [+] Encontrado Local: {data_fmt} - Página(s): {paginas_str}")
            resultados.append({'data': data_fmt, 'origem': 'Local', 'paginas': paginas_encontradas})
            
    return resultados

# ==========================================
# ORQUESTRADOR
# ==========================================
def realizar_busca(nome, ano=None, mes=None, dia=None, cpf=None):
    print(f"\n{'='*40}")
    print(f"BUSCA: {nome}")
    print(f"FILTROS: Ano={ano or 'Todos'}, Mês={mes or 'Todos'}, Dia={dia or 'Todos'}")
    print(f"{'='*40}\n")

    todos_resultados = []

    todos_resultados.extend(buscar_local_txt(nome, ano, mes, dia, cpf))

    print(f"\n--- RESUMO FINAL ---")
    print(f"Total de ocorrências: {len(todos_resultados)}")
    return todos_resultados

if __name__ == "__main__":
<<<<<<< HEAD
    realizar_busca("antonio da silva gomes")
=======
    # EXEMPLOS DE USO:
    
    # 1. Busca específica
    # realizar_busca(nome="IAN MATEUS ALVES RODRIGUES", ano=2025, mes=5)

    # 2. Busca apenas por nome em todos os anos (Demorado se houver muitos PDFs)
    # realizar_busca(nome="NOME DO ALVO")

    # 3. Busca por nome e dia específico, independente do mês ou ano
    realizar_busca("maria da conceição marques pinto")
>>>>>>> 70fa0bc3e5fdd32a8645d8fb783a4c9df27d3c8d
