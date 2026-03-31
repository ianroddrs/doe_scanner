import os
from pdf2image import convert_from_path
import pytesseract

# ==========================================
# CONFIGURAÇÕES DO SISTEMA (AJUSTE SE NECESSÁRIO)
# ==========================================
CAMINHO_POPPLER = r'poppler\Library\bin'
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

PASTA_ORIGEM = "pdfs_originais"
PASTA_DESTINO = "textos_ocr"

def aplicar_ocr_em_lote():
    if not os.path.exists(PASTA_ORIGEM):
        print(f"[!] A pasta '{PASTA_ORIGEM}' não existe. Rode o script 01 primeiro.")
        return

    if not os.path.exists(PASTA_DESTINO):
        os.makedirs(PASTA_DESTINO)
        print(f"[*] Pasta '{PASTA_DESTINO}' criada.")

    arquivos_pdf = [f for f in os.listdir(PASTA_ORIGEM) if f.endswith('.pdf')]
    total = len(arquivos_pdf)
    
    print(f"[*] Encontrados {total} PDFs para processar.")

    for index, arquivo in enumerate(arquivos_pdf, start=1):
        caminho_pdf = os.path.join(PASTA_ORIGEM, arquivo)
        
        # Cria o nome do novo arquivo (ex: 2007.01.01.DOE_ocr.txt)
        nome_base = arquivo.replace('.pdf', '')
        nome_txt = f"{nome_base}_ocr.txt"
        caminho_txt = os.path.join(PASTA_DESTINO, nome_txt)
        
        # Pula se já foi processado anteriormente
        if os.path.exists(caminho_txt):
            print(f"[{index}/{total}] PULANDO: '{arquivo}' (Já processado)")
            continue
            
        print(f"[{index}/{total}] PROCESSANDO OCR: '{arquivo}'...")
        
        try:
            # 1. Converte PDF para Imagens
            paginas = convert_from_path(
                caminho_pdf, 
                dpi=150, 
                poppler_path=CAMINHO_POPPLER,
                grayscale=True,
                thread_count=4
            )
            
            texto_completo = ""
            
            # 2. Aplica OCR em cada página
            for num_pagina, imagem in enumerate(paginas):
                texto_pagina = pytesseract.image_to_string(imagem, lang='por')
                texto_completo += f"\n--- PÁGINA {num_pagina + 1} ---\n"
                texto_completo += texto_pagina
                
            # 3. Salva o resultado no arquivo .txt
            with open(caminho_txt, 'w', encoding='utf-8') as f:
                f.write(texto_completo)
                
            print(f"    -> Salvo como '{nome_txt}'")
            
        except Exception as e:
            print(f"    [!] Erro ao processar '{arquivo}': {e}")

if __name__ == "__main__":
    print("INICIANDO PROCESSADOR OCR EM LOTE")
    print("Isso pode demorar bastante. Você pode pausar e continuar depois.")
    aplicar_ocr_em_lote()
    print("\n[OK] Processamento OCR finalizado!")