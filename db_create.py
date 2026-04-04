import os
import re
import sqlite3

def criar_banco_de_dados(nome_banco):
    """Cria a conexão com o banco e as tabelas necessárias."""
    conn = sqlite3.connect(nome_banco)
    cursor = conn.cursor()

    # Tabela de documentos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER,
            mes INTEGER,
            dia INTEGER,
            nome_pdf TEXT UNIQUE
        )
    ''')

    # Tabela de páginas (com chave estrangeira vinculada ao documento)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS paginas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            documento_id INTEGER,
            numero_pagina INTEGER,
            conteudo TEXT,
            FOREIGN KEY (documento_id) REFERENCES documentos (id) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    return conn

def processar_arquivos(diretorio, conn):
    """Lê os arquivos da pasta e os insere no banco de dados."""
    cursor = conn.cursor()
    
    # Expressão regular para encontrar e capturar os marcadores e o número da página
    # Ex: captura o "1" dentro de "--- PÁGINA 1 ---"
    padrao_pagina = re.compile(r'---\s*PÁGINA\s+(\d+)\s*---', re.IGNORECASE)

    for nome_arquivo in os.listdir(diretorio):
        if not nome_arquivo.endswith('_ocr.txt'):
            continue

        caminho_completo = os.path.join(diretorio, nome_arquivo)

        # 1. Extrair metadados do nome do arquivo (ex: 1980.12.31.DOE_ocr.txt)
        partes = nome_arquivo.split('.')
        if len(partes) < 4:
            print(f"Ignorando arquivo com formato inesperado: {nome_arquivo}")
            continue
            
        ano = int(partes[0])
        mes = int(partes[1])
        dia = int(partes[2])
        
        # Reconstrói o nome do arquivo PDF
        nome_pdf = f"{ano}.{mes:02d}.{dia:02d}.DOE.pdf"

        # 2. Inserir na tabela 'documentos'
        try:
            cursor.execute('''
                INSERT INTO documentos (ano, mes, dia, nome_pdf)
                VALUES (?, ?, ?, ?)
            ''', (ano, mes, dia, nome_pdf))
            
            # Recupera o ID gerado para este documento (chave primária)
            documento_id = cursor.lastrowid

        except sqlite3.IntegrityError:
            print(f"O documento {nome_pdf} já existe no banco. Pulando...")
            continue

        # 3. Ler o arquivo txt
        with open(caminho_completo, 'r', encoding='utf-8') as file:
            conteudo_completo = file.read()

        # O re.split divide o texto usando o marcador, mas mantém o número capturado (\d+).
        # A lista resultante fica parecida com:
        # ['texto ignorado do inicio', '1', 'conteudo pag 1', '2', 'conteudo pag 2', ...]
        partes_texto = padrao_pagina.split(conteudo_completo)

        # 4. Iterar sobre as páginas fatiadas e inserir na tabela 'paginas'
        # Começamos do índice 1 (onde estão os números) e pulamos de 2 em 2
        for i in range(1, len(partes_texto), 2):
            numero_pagina = int(partes_texto[i])
            # O conteúdo da página está no índice imediatamente a seguir
            conteudo_pagina = partes_texto[i+1].strip()

            # Evita inserir páginas que porventura estejam vazias
            if conteudo_pagina:
                cursor.execute('''
                    INSERT INTO paginas (documento_id, numero_pagina, conteudo)
                    VALUES (?, ?, ?)
                ''', (documento_id, numero_pagina, conteudo_pagina))

    conn.commit()
    print("\nProcessamento concluído com sucesso!")

# --- Execução do script ---
if __name__ == '__main__':
    DIRETORIO_OCR = 'textos_ocr'
    NOME_BANCO = 'diarios_oficiais.db'
    
    # Verifica se a pasta existe antes de tentar ler
    if not os.path.exists(DIRETORIO_OCR):
        print(f"Erro: A pasta '{DIRETORIO_OCR}' não foi encontrada no diretório atual.")
    else:
        print(f"Iniciando a leitura da pasta '{DIRETORIO_OCR}'...")
        conexao = criar_banco_de_dados(NOME_BANCO)
        processar_arquivos(DIRETORIO_OCR, conexao)
        conexao.close()