import os
import re
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from pdf2image import convert_from_path
import pytesseract
import multiprocessing
import PyPDF2

# ==========================================
# CONFIGURAÇÕES DO SISTEMA E OCR
# ==========================================
CAMINHO_POPPLER = r'poppler\Library\bin'
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# ==========================================
# CONFIGURAÇÕES DE SCRAPING E PASTAS
# ==========================================
ANO_INICIO = 1980
BASE_URL = "https://www.ioepa.com.br/arquivos/"

PASTA_DESTINO_TXT = "textos_ocr"
PASTA_TEMP_PDF = "temp_pdfs"

# Configuração de paralelismo
NUCLEOS_DISPONIVEIS = multiprocessing.cpu_count()
WORKERS_SIMULTANEOS = max(1, NUCLEOS_DISPONIVEIS - 4)

def obter_ultimo_ano_disponivel():
    """Lê a página principal do IOEPA e descobre dinamicamente o último diretório de ano."""
    print("[*] Verificando último ano disponível no servidor...")
    try:
        response = requests.get(BASE_URL, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        anos = []
        for a in soup.find_all('a'):
            texto = a.text.strip()
            # Se o texto for um número de 4 dígitos, considera como ano
            if texto.isdigit() and len(texto) == 4:
                anos.append(int(texto))
                
        if anos:
            max_ano = max(anos)
            print(f"[*] Último ano detectado no site: {max_ano}")
            return max_ano
    except Exception as e:
        print(f"[!] Erro ao detectar o último ano. Usando ano padrão (2026): {e}")
        
    return 2026 # Fallback caso o site saia do ar na raiz

def limpar_texto_extraido(texto):
    """Limpa e formata o texto extraído (serve tanto para OCR quanto Texto Nativo)."""
    blocos = re.split(r'(\n\s*\n|--- PÁGINA \d+ ---)', texto)
    blocos_processados = []
    
    for bloco in blocos:
        if "--- PÁGINA" in bloco:
            blocos_processados.append(bloco.strip())
            continue
        if not bloco.strip():
            continue

        conteudo = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', bloco)
        conteudo = conteudo.replace('\n', ' ')
        conteudo = re.sub(r'\s+', ' ', conteudo).strip()
        
        if conteudo:
            blocos_processados.append(conteudo)
    
    resultado = ""
    for b in blocos_processados:
        if "--- PÁGINA" in b:
            resultado += f"\n\n{b}\n\n"
        else:
            resultado += f"{b}\n\n"
            
    return resultado.strip()

def mapear_arquivos_pendentes(ano_fim):
    """Varre o site e retorna uma lista de PDFs que ainda não foram processados."""
    pdfs_pendentes = []
    
    print("\n[*] ETAPA 1: Mapeando arquivos no servidor...")
    
    for ano in range(ANO_INICIO, ano_fim + 1):
        url_ano = f"{BASE_URL}{ano}/"
        try:
            response = requests.get(url_ano, timeout=10)
            if response.status_code != 200:
                print(f"  [!] Ano {ano} não encontrado no servidor.")
                continue
        except Exception as e:
            print(f"  [!] Erro ao conectar no ano {ano}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        pdfs_ano = [a.get('href') for a in links if a.get('href') and a.get('href').endswith('.pdf')]
        
        for href in pdfs_ano:
            nome_arquivo = href.split('/')[-1]
            # Salvaremos todos com _ocr.txt para manter o padrão na pasta de destino
            caminho_txt = os.path.join(PASTA_DESTINO_TXT, nome_arquivo.replace('.pdf', '_ocr.txt'))
            
            # Só adiciona à lista se o TXT NÃO existir
            if not os.path.exists(caminho_txt):
                url_completa = href if href.startswith('http') else f"{url_ano}{nome_arquivo}"
                # Agora passamos o ANO na tupla para o worker saber qual método usar
                pdfs_pendentes.append((url_completa, nome_arquivo, ano))
                
        print(f"  -> Ano {ano} verificado. Encontrados {len(pdfs_ano)} PDFs.")
        
    return pdfs_pendentes

def extrair_texto_pdf_nativo(caminho_pdf):
    """Extrai texto rapidamente de PDFs nativos (2008 em diante) usando PyPDF2."""
    texto_bruto = ""
    with open(caminho_pdf, 'rb') as f:
        leitor = PyPDF2.PdfReader(f)
        for num_pagina, pagina in enumerate(leitor.pages):
            texto_pagina = pagina.extract_text()
            texto_bruto += f"\n--- PÁGINA {num_pagina + 1} ---\n"
            if texto_pagina:
                texto_bruto += texto_pagina + "\n"
    return texto_bruto

def baixar_e_processar_pdf(dados):
    """Função executada pelos workers: Baixa o PDF, define a estratégia de extração, salva TXT e deleta o PDF."""
    url, nome_arquivo, ano = dados
    nome_txt = nome_arquivo.replace('.pdf', '_ocr.txt')
    caminho_txt = os.path.join(PASTA_DESTINO_TXT, nome_txt)
    caminho_pdf_temp = os.path.join(PASTA_TEMP_PDF, nome_arquivo)
    
    # Double check preventivo
    if os.path.exists(caminho_txt):
        return f"PULANDO: '{nome_arquivo}' (TXT já existe)"

    try:
        # 1. Download do PDF para a pasta temporária
        resposta_pdf = requests.get(url, stream=True, timeout=30)
        resposta_pdf.raise_for_status()
        
        with open(caminho_pdf_temp, 'wb') as f:
            for chunk in resposta_pdf.iter_content(chunk_size=8192):
                f.write(chunk)

        texto_bruto = ""
        modo_extracao = ""

        # 2. Estratégia de Extração Baseada no Ano
        if ano <= 2007:
            modo_extracao = "OCR-IMAGEM"
            paginas = convert_from_path(
                caminho_pdf_temp, 
                dpi=150, 
                poppler_path=CAMINHO_POPPLER,
                grayscale=True,
                thread_count=1 
            )
            for num_pagina, imagem in enumerate(paginas):
                texto_pagina = pytesseract.image_to_string(imagem, lang='por')
                texto_bruto += f"\n--- PÁGINA {num_pagina + 1} ---\n"
                texto_bruto += texto_pagina
        else:
            modo_extracao = "TEXTO-NATIVO"
            texto_bruto = extrair_texto_pdf_nativo(caminho_pdf_temp)
            
        # 3. Limpa a formatação
        texto_limpo = limpar_texto_extraido(texto_bruto)
            
        # 4. Salva o resultado final (.txt)
        with open(caminho_txt, 'w', encoding='utf-8') as f:
            f.write(texto_limpo)
            
        return f"FINALIZADO ({modo_extracao}): '{nome_arquivo}'"
        
    except Exception as e:
        return f"ERRO em '{nome_arquivo}': {e}"
        
    finally:
        # 5. GARANTIA: Remove o PDF original independentemente de sucesso ou erro
        if os.path.exists(caminho_pdf_temp):
            try:
                os.remove(caminho_pdf_temp)
            except Exception as e:
                pass # Falhas de lock de arquivo no Windows podem acontecer, passamos silenciosamente

def main():
    print("="*60)
    print("INICIANDO EXTRATOR INTELIGENTE IOEPA")
    print("="*60)
    
    # Cria as pastas necessárias
    for pasta in [PASTA_DESTINO_TXT, PASTA_TEMP_PDF]:
        if not os.path.exists(pasta):
            os.makedirs(pasta)
            print(f"[*] Pasta '{pasta}' criada.")

    # Descobre dinamicamente até que ano processar
    ano_fim = obter_ultimo_ano_disponivel()

    # Etapa 1: Obter a lista de PDFs
    pdfs_pendentes = mapear_arquivos_pendentes(ano_fim)
    total_pendentes = len(pdfs_pendentes)
    
    if total_pendentes == 0:
        print("\n[OK] Nenhum arquivo novo para processar. Todos os textos já foram extraídos.")
        return

    # Etapa 2: Processamento paralelo
    print(f"\n[*] ETAPA 2: Iniciando Processamento de {total_pendentes} arquivos.")
    print(f"[*] Utilizando {WORKERS_SIMULTANEOS} processos simultâneos.")
    print("-" * 60)

    # Usa ProcessPoolExecutor para máximo desempenho
    with concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS_SIMULTANEOS) as executor:
        futuros = {executor.submit(baixar_e_processar_pdf, dados): dados for dados in pdfs_pendentes}
        
        processados = 0
        for futuro in concurrent.futures.as_completed(futuros):
            processados += 1
            resultado = futuro.result()
            print(f"[{processados}/{total_pendentes}] {resultado}")

    print("\n" + "="*60)
    print("[OK] Processo de extração finalizado!")
    print("="*60)

if __name__ == "__main__":
    main()