from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _spark():
    # Dentro do Databricks devolve a sessão do próprio notebook ou Job.
    return SparkSession.builder.getOrCreate()


def garantir_chave_unica(df, chave: list[str], nome: str) -> None:
    """Falha com asserção se a chave primária se repetir, impedindo duplicações silenciosas."""
    duplicadas = df.groupBy(*chave).count().where("count > 1").count()
    assert duplicadas == 0, f"{nome}: {duplicadas} chaves {chave} repetidas"
    print(f"{nome}: chave {chave} única - OK")


def tags_silver(fonte: str, dominio: str) -> dict[str, str]:
    """Gera o dicionário de tags padrão de governança para a camada Silver."""
    return {"camada": "silver", "fonte": fonte, "dominio": dominio, "dados_pessoais": "nao"}


def texto_sql(valor) -> str:
    """Escapa um texto para uso como literal SQL entre aspas simples."""
    return str(valor).replace("\\", "\\\\").replace("'", "\\'").replace("\n", " / ")


def aplicar_tags(tabela: str, tags: dict, coluna: str | None = None) -> None:
    """Aplica tags do Unity Catalog numa tabela ou, se informada, numa coluna dela."""
    if not tags:
        return
    pares = ", ".join(f"'{k}' = '{texto_sql(v)}'" for k, v in tags.items())
    alvo = f"ALTER TABLE {tabela} ALTER COLUMN `{coluna}`" if coluna else f"ALTER TABLE {tabela}"
    _spark().sql(f"{alvo} SET TAGS ({pares})")


def criar_tabela(df, tabela: str, descricao: str, propriedades: dict, tags: dict, comentarios: dict | None = None) -> None:
    """Cria a tabela já governada.

    Descrição, propriedades e comentários de coluna entram no próprio CREATE, junto com
    os dados, e as tags são aplicadas logo em seguida.
    """
    spark = _spark()
    comentarios = comentarios or {}
    df = df.select(*[
        F.col(f"`{c}`").alias(c, metadata={"comment": comentarios[c]}) if c in comentarios else F.col(f"`{c}`")
        for c in df.columns
    ])
    df.createOrReplaceTempView("origem_tabela")
    props = ", ".join(f"'{k}' = '{texto_sql(v)}'" for k, v in propriedades.items())
    spark.sql(f"""
        CREATE OR REPLACE TABLE {tabela}
        COMMENT '{texto_sql(descricao)}'
        TBLPROPERTIES ({props})
        AS SELECT * FROM origem_tabela
    """)
    # Garantia: se algum comentário do schema não tiver sido gravado, aplica explicitamente.
    gravados = {f.name: f.metadata.get("comment") for f in spark.table(tabela).schema.fields}
    for coluna, texto in comentarios.items():
        if coluna in gravados and not gravados[coluna]:
            spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN `{coluna}` COMMENT '{texto_sql(texto)}'")
    aplicar_tags(tabela, tags)
