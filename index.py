import requests
import psycopg2
import psycopg2.extras # Importante para inserção em massa
import json
import random
from datetime import datetime, timedelta

# ==========================================================
# 1. CONFIGURAÇÕES & CHAVES
# ==========================================================

SB_HOST = "aws-1-us-east-2.pooler.supabase.com"
SB_DB   = "postgres"
SB_USER = "postgres.iztzyvygulxlavixngeo"
SB_PASS = "Lukinha2009@"
SB_PORT = "6543"

ABM_USER = "lucas"
ABM_PASS = "Lukinha2009"
URL_BASE = "https://abmtecnologia.abmprotege.net"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def log(msg, type_log='inf'):
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = "🔵"
    if type_log == 'err': prefix = "🔴"
    elif type_log == 'suc': prefix = "🟢"
    print(f"{prefix} [{timestamp}] {msg}")

def safe_float(val):
    try:
        if val is None: return 0.0
        return float(val)
    except: return 0.0

# ==========================================================
# 2. LOGIN E TOKEN
# ==========================================================

def realizar_login():
    try:
        session.get(f"{URL_BASE}/emp/abmtecnologia")
        resp = session.post(f"{URL_BASE}/emp/abmtecnologia", 
            data={"login": ABM_USER, "senha": ABM_PASS, "password": ABM_PASS},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return "emp/abmtecnologia" not in resp.url
    except: return False

def obter_token():
    try:
        resp = session.post(f"{URL_BASE}/token/Api_ftk4", 
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": f"{URL_BASE}/dashboard_controller"}
        )
        return resp.json().get('access_token')
    except: return None

# ==========================================================
# 3. PROCESSAMENTO DE DADOS (Tudo na memória local)
# ==========================================================

def preparar_dados(raw_json):
    dados_para_inserir = []
    
    if raw_json:
        for i in raw_json:
            sub = i.get('sub_table', [])
            if not sub: continue
            for s in sub:
                for t in s.get('sub_table_infracao', []):
                    if t.get('tipo_infracao') in ["Motor Ocioso", "Banguela"]: continue
                    for d in t.get('infracoes', []):
                        end_data = d.get('endereco', {}) or {}
                        
                        # --- PROCESSAMENTO LOCAL (INSTANTÂNEO) ---
                        lat = safe_float(end_data.get('lat'))
                        lon = safe_float(end_data.get('lon'))
                        
                        # Gera Link Google
                        if lat == 0 or lon == 0:
                            link_maps = "Sem Coordenadas"
                        else:
                            # Link corrigido para abrir direto no Maps
                            link_maps = f"https://www.google.com/maps?q={lat},{lon}"

                        # Formata Data
                        try:
                            dt_obj = datetime.strptime(d.get('data'), '%d/%m/%Y %H:%M:%S')
                            data_fmt = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            data_fmt = None

                        # Gera ID Único
                        id_original = str(d.get('id_infracao'))
                        sufixo = f"_{random.randint(10000,99999)}"
                        id_final = f"{id_original}{sufixo}"

                        # Cria a Tupla (ordem exata das colunas no banco)
                        # (codigo_ativo, placa, motorista, total_infracoes, total_penalidades, tipo, id_ext, data, vel, pen, lat, lon, endereco)
                        registro = (
                            i.get('descricao_ativo', ''),
                            i.get('tag_ativo', ''),
                            s.get('descricao_motorista', ''),
                            int(i.get('total_infracoes', 0)),
                            int(i.get('total_penalidade', 0)),
                            t.get('tipo_infracao'),
                            id_final,
                            data_fmt,
                            int(d.get('velocidade', 0)),
                            d.get('penalidade', ''),
                            lat,
                            lon,
                            link_maps
                        )
                        dados_para_inserir.append(registro)
    
    return dados_para_inserir

# ==========================================================
# 4. SALVAMENTO OTIMIZADO (Bulk Insert)
# ==========================================================

def salvar_em_lote(lista_dados):
    if not lista_dados:
        log("Nenhum dado para salvar.", "warn")
        return

    log(f"💾 Iniciando upload de {len(lista_dados)} registros...", "inf")
    
    try:
        # Conexão ÚNICA (não precisa de pool para script sequencial em lote)
        conn = psycopg2.connect(host=SB_HOST, database=SB_DB, user=SB_USER, password=SB_PASS, port=SB_PORT)
        cursor = conn.cursor()

        sql = """
            INSERT INTO relatorio_infracoes 
            (codigo_ativo, placa, motorista, total_infracoes_geral, total_penalidades_geral, tipo_infracao, id_infracao_externo, data_infracao, velocidade, penalidade_valor, latitude, longitude, endereco)
            VALUES %s
        """
        
        # execute_values faz a mágica: converte a lista em um insert gigante
        psycopg2.extras.execute_values(
            cursor, sql, lista_dados, template=None, page_size=1000
        )
        
        conn.commit()
        log(f"✅ Sucesso! {len(lista_dados)} registros salvos de uma vez.", "suc")
        
        cursor.close()
        conn.close()

    except Exception as e:
        log(f"Erro no Banco de Dados: {e}", "err")

# ==========================================================
# 5. MAIN
# ==========================================================

def main():
    # 1. Login
    if not realizar_login(): log("Falha Login", "err"); return
    token = obter_token()
    if not token: log("Falha Token", "err"); return

    dt_str = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
    log(f"📅 Data Coleta: {dt_str}")

    # 2. Download JSON
    url = "https://api-fulltrack4.fulltrackapp.com/relatorio/DriverBehavior/gerar/"
    payload = {'id_cliente':'195577', 'id_motorista':'0', 'dt_inicial':f"{dt_str} 00:00:00", 'dt_final':f"{dt_str} 23:59:59", 'id_indice':'7259', 'id_usuario':'250095', 'visualizar_por':'ativo'}
    
    try:
        log("Baixando dados da API...", "inf")
        resp = session.post(url, data=payload, headers={"Authorization": f"Bearer {token}"})
        raw_json = resp.json()
    except Exception as e: 
        log(f"Erro Download JSON: {e}", "err"); return

    # 3. Prepara Dados (Memória - Rápido)
    log("Processando dados localmente...", "inf")
    dados = preparar_dados(raw_json)
    log(f"📋 Registros processados: {len(dados)}")

    # 4. Salva no Banco (Rede - Otimizado)
    salvar_em_lote(dados)

if __name__ == "__main__":
    main()
