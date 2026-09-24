"""Extração de fontes públicas e conversão para Parquet.

Execução externa ao cluster Databricks devido ao bloqueio de tráfego de saída (egress HTTP)
no Databricks Free Edition. Os arquivos Parquet gerados são destinados à landing zone do Unity Catalog.

Uso: python scripts_auxiliares/extrair_dados.py
"""

import difflib
import io
import json
import os
import re
import tempfile
import time
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd
import requests
import truststore

truststore.inject_into_ssl()  # Certificados nativos do Windows para compatibilidade com inspeção TLS corporativa

RAIZ = Path(__file__).resolve().parent.parent
PARQUET_DIR = Path(os.getenv("DATA_PARQUET_DIR", RAIZ / "data"))
MES_BOLSA_FAMILIA = os.getenv("MES_BOLSA_FAMILIA", "202405")
ANO_CENSO = int(os.getenv("ANO_CENSO", "2023"))

URLS = {
    "municipios_ibge.json": "https://servicodados.ibge.gov.br/api/v1/localidades/municipios",
    "populacao_municipios_ibge.json": "https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6",
    "ideb_anos_iniciais_municipios.xlsx": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_municipios_2023.xlsx",
    "ideb_anos_finais_municipios.xlsx": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_2023.xlsx",
    f"microdados_censo_escolar_{ANO_CENSO}.zip": f"https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{ANO_CENSO}.zip",
    "bolsa_familia.zip": f"https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/{MES_BOLSA_FAMILIA}",
}

# De-para de municípios com divergência de nomenclatura entre Portal da Transparência (SIAFI) e IBGE
RENOMEADOS = {
    ("RR", "SAO LUIZ"): 1400605,
    ("TO", "FORTALEZA DO TABOCAO"): 1708254,
    ("TO", "SAO VALERIO DA NATIVIDADE"): 1720499,
    ("PB", "SAO DOMINGOS DE POMBAL"): 2513968,
    ("PB", "SERIDO"): 2515401,
    ("RN", "ACU"): 2400208,
    ("RN", "ARES"): 2401206,
    ("PE", "DISTRITO ESTADUAL DE FERNANDO DE NORONHA"): 2605459,
    ("SP", "EMBU"): 3515004,
}


def baixar(nome: str, temp_dir: Path) -> Path:
    """Realiza download em diretório temporário com streaming em chunks e retry exponencial."""
    destino = temp_dir / nome
    if destino.exists() and destino.stat().st_size > 0:
        print(f"  {nome} (em cache local: {destino.stat().st_size / 1e6:.1f} MB)")
        return destino

    for tentativa in range(3):
        try:
            print(f"  Baixando {nome}...")
            with requests.get(URLS[nome], timeout=1800, stream=True) as r:
                r.raise_for_status()
                with open(destino, "wb") as f:
                    for chunk in r.iter_content(chunk_size=512 * 1024):
                        if chunk:
                            f.write(chunk)
            print(f"  {nome}: {destino.stat().st_size / 1e6:.1f} MB concluído")
            return destino
        except requests.RequestException as erro:
            print(f"  {nome}: tentativa {tentativa + 1}/3 falhou ({erro})")
            time.sleep(3)
    raise ConnectionError(f"Falha definitiva ao baixar {nome}")


def salvar(df: pd.DataFrame, nome: str) -> None:
    saida = PARQUET_DIR / f"{nome}.parquet"
    df.to_parquet(saida, index=False, compression="snappy")
    print(f"  {saida.name:34} {len(df):>9,} linhas x {len(df.columns):>3} col")


def normalizar(nome: str) -> str:
    """Normaliza texto para maiúsculas sem acentuação e sem caracteres especiais."""
    texto = unicodedata.normalize("NFKD", re.sub(r"\(.*?\)", " ", str(nome)))
    texto = texto.encode("ascii", "ignore").decode("ascii").upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", texto)).strip()


def uf_e_regiao(m: dict) -> tuple[str | None, str | None]:
    """Extrai UF e região contornando ausência de microrregião em Fernando de Noronha."""
    no = m.get("microrregiao", {}) or {}
    unidade = no.get("mesorregiao", {}).get("UF") if no else None
    if unidade is None and m.get("regiao-imediata"):
        unidade = m["regiao-imediata"]["regiao-intermediaria"]["UF"]
    return (unidade["sigla"], unidade["regiao"]["nome"]) if unidade else (None, None)


def descartar(caminho: Path) -> None:
    """Remove arquivo bruto baixado para liberar espaço em disco."""
    try:
        if caminho.exists():
            caminho.unlink(missing_ok=True)
            print(f"  [Descartado] {caminho.name}")
    except OSError as erro:
        print(f"  Aviso: falha ao remover arquivo temporário {caminho.name} ({erro})")


def extrair_ibge(temp_dir: Path) -> pd.DataFrame:
    arq_mun = baixar("municipios_ibge.json", temp_dir)
    linhas = []
    for m in json.loads(arq_mun.read_text(encoding="utf-8")):
        uf, regiao = uf_e_regiao(m)
        linhas.append({
            "codigo_municipio_ibge": int(m["id"]),
            "nome_municipio": m["nome"],
            "sigla_uf": uf,
            "nome_regiao": regiao,
        })
    df = pd.DataFrame(linhas)
    salvar(df, "municipios_ibge")
    descartar(arq_mun)

    arq_pop = baixar("populacao_municipios_ibge.json", temp_dir)
    series = json.loads(arq_pop.read_text(encoding="utf-8"))[0]["resultados"][0]["series"]
    salvar(
        pd.DataFrame([
            {
                "codigo_municipio_ibge": int(s["localidade"]["id"]),
                "ano": 2022,
                "populacao": int(s["serie"]["2022"]),
            }
            for s in series
        ]),
        "populacao_municipios",
    )
    descartar(arq_pop)
    return df


def extrair_ideb(temp_dir: Path) -> None:
    partes = []
    for arquivo, etapa in [
        ("ideb_anos_iniciais_municipios.xlsx", "Anos Iniciais"),
        ("ideb_anos_finais_municipios.xlsx", "Anos Finais"),
    ]:
        caminho = baixar(arquivo, temp_dir)
        parte = pd.read_excel(caminho, skiprows=9)  # Linhas 1 a 9 são títulos institucionais
        parte["etapa_ensino"] = etapa
        partes.append(parte[pd.to_numeric(parte["CO_MUNICIPIO"], errors="coerce").notna()])
        descartar(caminho)

    df = pd.concat(partes, ignore_index=True)
    df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype("int64")
    for coluna in [c for c in df.columns if c.startswith("VL_")]:
        df[coluna] = pd.to_numeric(df[coluna].astype(str).str.replace(",", "."), errors="coerce")
    salvar(df, "ideb_municipios")


def extrair_censo(temp_dir: Path) -> None:
    caminho_zip = baixar(f"microdados_censo_escolar_{ANO_CENSO}.zip", temp_dir)
    with zipfile.ZipFile(caminho_zip) as z:
        csv = next(n for n in z.namelist() if n.endswith(f"microdados_ed_basica_{ANO_CENSO}.csv"))
        with z.open(csv) as f:
            salvar(pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False), "censo_escolar_escolas")

        xlsx = next(n for n in z.namelist() if n.endswith(".xlsx"))
        dic = pd.read_excel(io.BytesIO(z.read(xlsx)), sheet_name="microdados_unidade_coleta", skiprows=6)

    descartar(caminho_zip)

    dic.columns = [str(c).strip() for c in dic.columns]
    dic = dic[dic["Nome da Variável"].notna()].rename(columns={
        "Nome da Variável": "nome_variavel",
        "Descrição da Variável": "descricao",
        "Tipo": "tipo",
        "Tam.(1)": "tamanho",
        "Categoria": "dominio_valores",
    })
    dic = dic[["nome_variavel", "descricao", "tipo", "tamanho", "dominio_valores"]]
    for coluna in ["nome_variavel", "descricao", "tipo", "dominio_valores"]:
        dic[coluna] = dic[coluna].astype(str).str.strip().replace({"nan": None})
    dic["tamanho"] = pd.to_numeric(dic["tamanho"], errors="coerce").astype("Int64")
    salvar(dic, "dicionario_censo_escolar")


def extrair_bolsa_familia(municipios: pd.DataFrame, temp_dir: Path) -> None:
    """Agrega os microdados por município em streaming (chunks de 500k linhas).
    
    Remove dados sensíveis (NIS, nomes de beneficiários) antes da carga no Lakehouse (conformidade LGPD).
    """
    caminho_zip = baixar("bolsa_familia.zip", temp_dir)
    with zipfile.ZipFile(caminho_zip) as z:
        nome_csv = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        with z.open(nome_csv) as f:
            blocos = [
                b.groupby(["UF", "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO"], as_index=False)
                 .agg(valor_total=("VALOR PARCELA", "sum"), quantidade_beneficiados=("VALOR PARCELA", "size"))
                for b in pd.read_csv(
                    f,
                    sep=";",
                    encoding="latin-1",
                    decimal=",",
                    chunksize=500_000,
                    usecols=["UF", "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO", "VALOR PARCELA"],
                )
            ]

    descartar(caminho_zip)

    df = (
        pd.concat(blocos, ignore_index=True)
        .groupby(["UF", "CÓDIGO MUNICÍPIO SIAFI", "NOME MUNICÍPIO"], as_index=False)
        .agg(valor_total=("valor_total", "sum"), quantidade_beneficiados=("quantidade_beneficiados", "sum"))
        .rename(columns={
            "UF": "sigla_uf",
            "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
            "NOME MUNICÍPIO": "nome_municipio",
        })
    )
    df["mes_referencia"] = MES_BOLSA_FAMILIA

    # Tradução SIAFI -> IBGE baseada em UF e nome normalizado
    de_para = municipios.assign(chave=municipios["nome_municipio"].map(normalizar))
    df["chave"] = df["nome_municipio"].map(normalizar)
    df = df.merge(de_para[["sigla_uf", "chave", "codigo_municipio_ibge"]], on=["sigla_uf", "chave"], how="left")

    for idx in df.index[df["codigo_municipio_ibge"].isna()]:
        uf, chave = df.at[idx, "sigla_uf"], df.at[idx, "chave"]
        candidatos = de_para[de_para["sigla_uf"] == uf]
        similar = difflib.get_close_matches(chave, candidatos["chave"].tolist(), n=1, cutoff=0.8)
        if (uf, chave) in RENOMEADOS:
            df.at[idx, "codigo_municipio_ibge"] = RENOMEADOS[(uf, chave)]
        elif similar:
            df.at[idx, "codigo_municipio_ibge"] = candidatos.loc[
                candidatos["chave"] == similar[0], "codigo_municipio_ibge"
            ].iloc[0]
        else:
            print(f"    Sem correspondência: {uf} '{chave}'")

    faltantes = df["codigo_municipio_ibge"].isna().sum()
    if faltantes > 0:
        raise ValueError(f"{faltantes} municípios sem código IBGE: verifique o de-para de nomes")

    df["codigo_municipio_ibge"] = df["codigo_municipio_ibge"].astype("int64")
    salvar(df.drop(columns=["chave"]), "bolsa_familia_brasil")


def main() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        municipios = extrair_ibge(temp_dir)
        extrair_ideb(temp_dir)
        extrair_censo(temp_dir)
        extrair_bolsa_familia(municipios, temp_dir)

    arquivos = list(PARQUET_DIR.glob("*.parquet"))
    total_mb = sum(p.stat().st_size for p in arquivos) / 1e6
    print(f"\nExtração concluída: {len(arquivos)} arquivos Parquet ({total_mb:.1f} MB) em {PARQUET_DIR}")


if __name__ == "__main__":
    main()