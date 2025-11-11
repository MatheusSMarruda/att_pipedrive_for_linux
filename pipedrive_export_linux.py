import os
import time
import requests
import pandas as pd
from pathlib import Path

# === Config ===
API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN", "COLOQUE_AQUI_O_TOKEN_OU_USE_ENV")
BASE_URL = "https://tempoenergia.pipedrive.com/v1"

# Diretório de saída (Linux)
BASE_DIR_SAIDA = Path(os.getenv("BASE_DIR_SAIDA", "/srv/tempoenergia/Gestao_DG"))
BASE_DIR_SAIDA.mkdir(parents=True, exist_ok=True)

ARQ_36 = BASE_DIR_SAIDA / "dados_pipedrive_venda_funil_36.xlsx"
ARQ_37 = BASE_DIR_SAIDA / "dados_pipedrive_venda_funil_37.xlsx"
ARQ_38 = BASE_DIR_SAIDA / "dados_pipedrive_venda_funil_38.xlsx"  # Novo funil

PIPELINES = [36, 37, 38]

# ================== COLETA PIPEDRIVE ==================
def buscar_deals(pipeline_id):
    url = f"{BASE_URL}/deals"
    params = {
        "api_token": API_TOKEN,
        "pipeline_id": pipeline_id,
        "start": 0,
        "limit": 500,
    }

    deals = []
    vistos = set()
    start = 0
    MAX_RETRIES = 5

    while True:
        params["start"] = start
        for tentativa in range(MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Erro de conexão {tentativa+1}/{MAX_RETRIES} (funil {pipeline_id}): {e}")
                if tentativa < MAX_RETRIES - 1:
                    espera = 2 ** tentativa
                    print(f"🔁 Repetindo em {espera}s…")
                    time.sleep(espera)
                    continue
                else:
                    print(f"❌ Falhou após {MAX_RETRIES} tentativas no funil {pipeline_id}.")
                    return deals

        data = payload.get("data") or []
        if not data:
            print(f"ℹ️ Nenhum dado em start={start} (funil {pipeline_id}).")
            break

        for d in data:
            did = d.get("id")
            if did in vistos:
                continue
            vistos.add(did)
            deals.append(d)

        pag = (payload.get("additional_data") or {}).get("pagination") or {}
        if not pag.get("more_items_in_collection"):
            break
        start = pag.get("next_start", start + len(data))

    print(f"✅ {len(deals)} negócios coletados no funil {pipeline_id}.")
    return deals


def buscar_deals_multiplos(pipelines):
    todos = []
    for p in pipelines:
        print(f"🔄 Buscando negócios no funil {p}…")
        d = buscar_deals(pipeline_id=p)
        print(f"✅ {len(d)} negócios no funil {p}.")
        todos.extend(d)
    return todos


def mapa_stages(pipeline_id: int):
    resp = requests.get(f"{BASE_URL}/stages", params={"api_token": API_TOKEN, "pipeline_id": pipeline_id}, timeout=30)
    resp.raise_for_status()
    stages = resp.json().get("data", []) or []
    return {str(stage["id"]): stage["name"] for stage in stages}

# ================== PROCESSAMENTO / EXPORTAÇÃO ==================
def main():
    deals = buscar_deals_multiplos(PIPELINES)

    campos_desejados = [
        "id", "title", "stage_id", "bc6e85bfd61a7f514b65fbf8d1f3a7bacefc7f56", "stage_change_time",
        "close_time", "user_id", "0e337215f1841654337f8b8f179c8fab49eeed4a", "465d3864f4099c2cbdbcfd079b5f9ef55dfbb62b",
        "9fe9cf54ba1179213d9baa8e43c830ce9f102dc0","590352702177f226733088bb21c275a10f341010",
        "fae8184ad9ee4befb23365ad84e47c76e03c6f71","f29736fac633e87f54f381b99b362adb1e7bb0ee",
        "a1aaa6b42e2227d7ddefeacb6ec56fd99582cd4f","cac182aec69c63b36ca9d10c9be0bab1ba13d754",
        "f3fa10c9b5c320f3915eacd406788b3afc1393bf","8c532ade194a563fe012c97342c9868944bd4f63",
        "53ffc52e6f99cc0ce30fb07666ffadd086180319","736773c3f0bdc8d0e924ddcf58e25ccfc1a72fde",
        "bba2ac4fe94f03ecdd992fb776f72920365333ac","fc3eca61c147c669c2bac7e9c45c7dfad719726a",
        "df48f449a64658a141039aacfde5edc7ccf410ee","8353ae96bda97a79e2512e7c2680a404ad9e629d",
        "fd95d501f5c6cc6cdf7477b31cb641fec64f9e23","13df2cda5af8200a9c529622802bbc0253216ec2",
        "e0097a58141aa034b4f0ae6781f55dae72447344","0cddc4521e3a29fe289023bde98cbebf1fa5ce4f",
        "478aa978bc98b58533da568c884e86e13ab00b7d","bf304a615af7b084a4859fc549d45cf85805a942",
        "92c18ce9c3693e21351e92094ec88176fe87b32f","5a504caf68f2aad0b556ad4d6512ae0bed75344d",
        "fb5a449d279eb875666e1b8556904e5b661769cc","64959a1b3dfb80e3db6298f7e31010de3515634f",
        "2615037472ae52af6860b9439f1464ddd99338ef","fe5c0601a8f776d56dca72022f804efa77725682",
        "b2836b320bcf3410f1683891cc08d439b036c8f6","02e097a06843fc0219d77e7f6368f06d8357c547",
        "38aa0841aabe0b710a641a7a7916dbb57a26b268","df644a2077e8d056b3534fc61ffd79dd23c1acc0",
        "4a63aaad316c6774ee091333d26c425a45fc0ff4","19f28c6c6d4c4955b15ea969e49622af4317b71c",
        "3e7c86788e156ea79a89c2412f0c5a23eb82482","e11b45e359ccc5a312a014a35e6f16a34579fe09",
        "33b63a71771e1fc60dc46d2c7d05433b47feedc7","cc48a34adc82ae49ff2ccbb81149c7985bfe544b",
        "8926402668f580d66fa24de48d29e4bf4d599931","63d244e4f8e35cc95dd0161741b0979def66e1aa",
        "7212f66e3421c7c6a45f1b79003a9c32414cc5c3","6b6e40653c69b97f55f72220c734fc73f000bb5e",
        "76385daca5d058a7f75c5691888ddbeb708398ed","ffe7e7beb1539a35333e215247db8fe335c72741",
        "f5b2e3f98d860f7420d4eb6fd709f14feff6c5e2","09aa880f1c8610bd0c18d75bb0161b2d520eb4d9",
        "a8bc2f318729c3dbe8d46f7790b5684f0d55b02e","3bc3ceca7be17392703c91dcae7007ecefb458fe",
        "0f6581e34ce553ca8c691d50c767a64b1843b829","717483f0900eac4b17d7ed05cb5d8292d081cb00",
        "e400765cff63eade9605a491a105418fb4c2f389","791462d03e80c6981c9b7852c21d57f4ad5e6bd9",
        "ef1347fdba636ead88f61566f8def53cddbe8cec","56d2c02b3d7602f627d8936e921f1acd3a0cb3db",
        "d1024fa19d504fc80b9e0a5b00a61ae4fec020c2","4a63aaad316c6774ee091333d26c425a45fc0ff4",
        "origin_id","8ebffeda681e402586769dc15c3916760fb81ef3","add_time","value","channel",
        "93df664878ce08f58067f382e1c134bed803ce53","b4233a37174ad172b79ec854faab5d280ec78fa3",
        "b2624651e42bc6b90dd3988d9b8e3d31260a2aa2","status","lost_reason","deb4d6ce978779304d95add8260d43d14051d6a1",
        "pipeline_id","9fe715b9c83f91c5131aa7cf580c20f033912228","ca61a683d1602938a67b5431d929affc35a8c486",
        "f6671d52cf7acaa5c7ee0370fd43e064078f913e","d8daf4840e74c20538e0604230b696029cf67cc5",
        "5d6f2509ce01acf1f143dde2bd8b9bfbc22fd3c1"
    ]

    colunas_nomes = {
        "title": "Título","stage_id": "Etapa","bc6e85bfd61a7f514b65fbf8d1f3a7bacefc7f56": "Data de Alteração de Funil",
        "stage_change_time": "Última alteração de etapa","close_time": "Negócio fechado em","user_id": "Proprietário",
        "0e337215f1841654337f8b8f179c8fab49eeed4a": "Telefones do Consumidor Final (separados por , para mais de um telefone)",
        "465d3864f4099c2cbdbcfd079b5f9ef55dfbb62b": "Nome do Primeiro Representante",
        "9fe9cf54ba1179213d9baa8e43c830ce9f102dc0": "E-mails do Consumidor Final (separados por , para mais de um e-mail)",
        "590352702177f226733088bb21c275a10f341010": "N° Instalação","fae8184ad9ee4befb23365ad84e47c76e03c6f71": "Tipo do Plano",
        "f29736fac633e87f54f381b99b362adb1e7bb0ee": "Cidade da Instalação","a1aaa6b42e2227d7ddefeacb6ec56fd99582cd4f": "Estado da Instalação",
        "cac182aec69c63b36ca9d10c9be0bab1ba13d754": "Número do Cliente","f3fa10c9b5c320f3915eacd406788b3afc1393bf": "Consumo Médio (KWh)",
        "8c532ade194a563fe012c97342c9868944bd4f63": "Número do (local) da Instalação", "53ffc52e6f99cc0ce30fb07666ffadd086180319": "Complemento da Instalação",
        "736773c3f0bdc8d0e924ddcf58e25ccfc1a72fde": "Distribuidora (COELBA)","bba2ac4fe94f03ecdd992fb776f72920365333ac": "Consórcio",
        "fc3eca61c147c669c2bac7e9c45c7dfad719726a": "Consórcio Líder","df48f449a64658a141039aacfde5edc7ccf410ee": "Modalidade de Compensação",
        "8353ae96bda97a79e2512e7c2680a404ad9e629d": "RG e Orgão Emissor do primeiro Representante",
        "fd95d501f5c6cc6cdf7477b31cb641fec64f9e23": "Data de Nascimento do Primeiro Representante",
        "13df2cda5af8200a9c529622802bbc0253216ec2": "Estado Civil do Primeiro Representante",
        "e0097a58141aa034b4f0ae6781f55dae72447344": "Nacionalidade do Primeiro Representante",
        "0cddc4521e3a29fe289023bde98cbebf1fa5ce4f": "Profissão do Primeiro Representante",
        "478aa978bc98b58533da568c884e86e13ab00b7d": "CEP do Primeiro Representante",
        "bf304a615af7b084a4859fc549d45cf85805a942": "Complemento do Primeiro Representante",
        "92c18ce9c3693e21351e92094ec88176fe87b32f": "Numero do Primeiro Representante",
        "5a504caf68f2aad0b556ad4d6512ae0bed75344d": "Nome do Segundo Representante",
        "fb5a449d279eb875666e1b8556904e5b661769cc": "RG e Orgão Emissor do Segundo Representante",
        "64959a1b3dfb80e3db6298f7e31010de3515634f": "CPF do Segundo Representante",
        "2615037472ae52af6860b9439f1464ddd99338ef": "Estado Civil do Segundo Representante",
        "fe5c0601a8f776d56dca72022f804efa77725682": "Nacionalidade do Segundo Representante",
        "b2836b320bcf3410f1683891cc08d439b036c8f6": "Profissão do Segundo Representante",
        "02e097a06843fc0219d77e7f6368f06d8357c547": "CEP do Segundo Representante",
        "38aa0841aabe0b710a641a7a7916dbb57a26b268": "Complemento do Segundo Representante",
        "df644a2077e8d056b3534fc61ffd79dd23c1acc0": "Número do Segundo Representante",
        "4a63aaad316c6774ee091333d26c425a45fc0ff4": "Nome da Primeira Testemunha",
        "19f28c6c6d4c4955b15ea969e49622af4317b71c": "CPF da Primeira Testemunha",
        "3e7c86788e156ea79a89c2412f0c5a23eb82482": "Nome da Segunda Testemunha",
        "e11b45e359ccc5a312a014a35e6f16a34579fe09": "CPF da Segunda Testemunha",
        "33b63a71771e1fc60dc46d2c7d05433b47feedc7": "Contato Financeiro",
        "cc48a34adc82ae49ff2ccbb81149c7985bfe544b": "Telefone do Contato Financeiro",
        "8926402668f580d66fa24de48d29e4bf4d599931": "E-mail do Contato Financeiro",
        "63d244e4f8e35cc95dd0161741b0979def66e1aa": "Tarifa com Desconto R$",
        "7212f66e3421c7c6a45f1b79003a9c32414cc5c3": "Cidade do Representante",
        "6b6e40653c69b97f55f72220c734fc73f000bb5e": "Estado do Representante",
        "76385daca5d058a7f75c5691888ddbeb708398ed": "CNPJ Consórcio",
        "ffe7e7beb1539a35333e215247db8fe335c72741": "CNPJ Consórcio Líder",
        "f5b2e3f98d860f7420d4eb6fd709f14feff6c5e2": "NIRE Consórcio",
        "09aa880f1c8610bd0c18d75bb0161b2d520eb4d9": "Endereço Consórcio Líder",
        "a8bc2f318729c3dbe8d46f7790b5684f0d55b02e": "Cidade Consórcio Líder",
        "3bc3ceca7be17392703c91dcae7007ecefb458fe": "Estado Consórcio Líder",
        "0f6581e34ce553ca8c691d50c767a64b1843b829": "N° Consórcio Líder",
        "717483f0900eac4b17d7ed05cb5d8292d081cb00": "Complemento Consórcio Líder",
        "e400765cff63eade9605a491a105418fb4c2f389": "Bairro Consórcio Líder",
        "791462d03e80c6981c9b7852c21d57f4ad5e6bd9": "CEP Consórcio Líder",
        "ef1347fdba636ead88f61566f8def53cddbe8cec": "Beneficio Estimado",
        "56d2c02b3d7602f627d8936e921f1acd3a0cb3db": "Beneficio em Boleto",
        "d1024fa19d504fc80b9e0a5b00a61ae4fec020c2": "Fidelidade",
        "4a63aaad316c6774ee091333d26c425a45fc0ff4": "Estado Consórcio",
        "origin_id": "ID de origem", "8ebffeda681e402586769dc15c3916760fb81ef3": "Estado",
        "add_time": "Negócio criado em", "value": "Valor", "channel": "Canal de origem",
        "93df664878ce08f58067f382e1c134bed803ce53": "Finder (Origem da Fatura)",
        "b4233a37174ad172b79ec854faab5d280ec78fa3": "Lead (Origem)",
        "b2624651e42bc6b90dd3988d9b8e3d31260a2aa2": "Inicio do Fornecimento GD",
        "status": "Status", "lost_reason": "Motivo da perda",
        "deb4d6ce978779304d95add8260d43d14051d6a1": "Nº de Unidades",
        "9fe715b9c83f91c5131aa7cf580c20f033912228": "Média de Consumo (KWh)",
        "ca61a683d1602938a67b5431d929affc35a8c486": "kWh Contratado",
        "f6671d52cf7acaa5c7ee0370fd43e064078f913e": "kWh Não Compensavel",
        "d8daf4840e74c20538e0604230b696029cf67cc5": "Desconto na Tarifa (%)",
        "5d6f2509ce01acf1f143dde2bd8b9bfbc22fd3c1": "Percentual Concedido"
    }

    print("Transformando dados em DataFrame…")
    df_raw = pd.DataFrame(deals)
    if df_raw.empty:
        print("Nenhum negócio encontrado.")
        return

    if "id" in df_raw.columns:
        antes = len(df_raw); df_raw = df_raw.drop_duplicates(subset=["id"], keep="last"); depois = len(df_raw)
        print(f"Deduplicação global por id: {antes} → {depois}")

    cols_existentes = [c for c in campos_desejados if c in df_raw.columns]
    df = df_raw[cols_existentes].copy()

    print("Substituindo códigos por texto nos campos dropdown/set…")
    resp = requests.get(f"{BASE_URL}/dealFields", params={"api_token": API_TOKEN}, timeout=30)
    resp.raise_for_status()
    for campo in resp.json().get("data", []) or []:
        key = campo.get("key")
        if key not in df.columns:
            continue
        ftype = campo.get("field_type")
        if ftype == "enum":
            mapping = {str(opt["id"]): opt["label"] for opt in (campo.get("options") or [])}
            df[key] = df[key].astype(str).map(mapping).fillna(df[key])
        elif ftype == "set":
            mapping = {str(opt["id"]): opt["label"] for opt in (campo.get("options") or [])}
            def traduz_lista(val):
                if val in (None, "", []): return ""
                if isinstance(val, list):
                    return ", ".join([mapping.get(str(v), str(v)) for v in val])
                if isinstance(val, str) and "," in val:
                    return ", ".join([mapping.get(x.strip(), x.strip()) for x in val.split(",") if x.strip()])
                return mapping.get(str(val), str(val))
            df[key] = df[key].apply(traduz_lista)

    for p, caminho in [(36, ARQ_36), (37, ARQ_37), (38, ARQ_38)]:
        df_p = df[df["pipeline_id"] == p].copy() if "pipeline_id" in df.columns else pd.DataFrame()
        if df_p.empty:
            print(f"Funil {p}: sem registros para exportar.")
            continue

        if "id" in df_p.columns:
            antes = len(df_p); df_p = df_p.drop_duplicates(subset=["id"], keep="last"); depois = len(df_p)
            print(f"Funil {p}: dedup por id {antes} → {depois}")

        if "stage_id" in df_p.columns:
            print(f"Mapeando stage_id → nome (funil {p})…")
            stage_map = mapa_stages(p)
            df_p["stage_id"] = df_p["stage_id"].astype(str).map(stage_map).fillna(df_p["stage_id"])

        for col_drop in ["pipeline_id", "id"]:
            if col_drop in df_p.columns:
                df_p = df_p.drop(columns=[col_drop])

        df_p = df_p.rename(columns=colunas_nomes).replace({None: ""})
        print(f"Salvando Excel do funil {p}: {caminho}")
        df_p.to_excel(caminho, index=False)

    print("✅ Exportação por funil concluída!")

# ================== REFRESH NO LIBREOFFICE (UNO) ==================
def refresh_calc_with_uno(xlsx_path: Path, host="127.0.0.1", port=2002, timeout=30):
    """
    Abre um .xlsx no LibreOffice (headless via UNO), força atualização de links/cálculos e salva.
    Requer um processo soffice escutando em socket. Se não estiver rodando, tenta subir um.
    """
    import subprocess, shutil, time as _t
    try:
        import uno
        from com.sun.star.beans import PropertyValue
        from com.sun.star.document.UpdateDocMode import QUIET_UPDATE  # 1
        from unohelper import systemPathToFileUrl
    except Exception as e:
        raise RuntimeError("Módulos UNO não instalados. Instale 'libreoffice' e 'python3-uno'.") from e

    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        raise RuntimeError("LibreOffice não encontrado (soffice). Instale 'libreoffice'.")

    # Sobe o LibreOffice headless se não estiver ouvindo
    def _start_office():
        return subprocess.Popen([
            soffice, "--headless",
            f'--accept=socket,host={host},port={port};urp;',
            "--norestore", "--nolockcheck", "--nodefault"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Tenta conectar (com tentativas)
    import socket
    def _can_connect():
        s = socket.socket(); s.settimeout(0.5)
        try:
            s.connect((host, port)); return True
        except Exception:
            return False
        finally:
            s.close()

    if not _can_connect():
        _proc = _start_office()
        # pequeno aguardo para subir
        for _ in range(30):
            if _can_connect(): break
            _t.sleep(0.5)

    # Conecta no UNO
    local_ctx = uno.getComponentContext()
    resolver = local_ctx.ServiceManager.createInstanceWithContext(
        "com.sun.star.bridge.UnoUrlResolver", local_ctx)
    ctx = resolver.resolve(f"uno:socket,host={host},port={port};urp;StarOffice.ComponentContext")
    smgr = ctx.ServiceManager
    desktop = smgr.createInstanceWithContext("com.sun.star.frame.Desktop", ctx)

    # Propriedades: oculto + atualizar links em modo silencioso
    def _prop(name, value): 
        pv = PropertyValue(); pv.Name = name; pv.Value = value; return pv

    file_url = systemPathToFileUrl(str(xlsx_path))
    args = (
        _prop("Hidden", True),
        _prop("ReadOnly", False),
        _prop("UpdateDocMode", QUIET_UPDATE),  # atualiza links sem prompts
    )

    doc = desktop.loadComponentFromURL(file_url, "_blank", 0, args)

    # Força recálculo completo
    # A interface XCalculatable está disponível em planilhas
    try:
        doc.calculateAll()
    except Exception:
        pass  # em alguns casos, o recálculo já ocorre na abertura

    # Salva e fecha
    doc.store()
    doc.close(True)
    print(f"🔄 LibreOffice: atualização concluída para {xlsx_path}")

if __name__ == "__main__":
    # 1) roda a extração + exportação
    main()

    # 2) atualiza o arquivo “consolidador” (se você precisa disso no Linux)
    #    Ajuste o nome abaixo para o arquivo certo (ex.: algum painel que referencia as 3 saídas)
    excel_path = BASE_DIR_SAIDA / "Dados_Gestao_DG.xlsx"
    if excel_path.exists():
        refresh_calc_with_uno(excel_path)
    else:
        print(f"ℹ️ Aviso: {excel_path} não existe (pulei atualização no LibreOffice).")
