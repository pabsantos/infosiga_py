import pandas as pd
import zipfile
import os
import tempfile
import shutil
from janitor import clean_names
import importlib.resources

# Descompactar o arquivo
name_zip = "dados_infosiga.zip"
raw_folder = "data-raw"
path_zip = (
    importlib.resources
    .files("infosiga_py")
    .joinpath(raw_folder, name_zip)
)

tempdir = tempfile.mkdtemp()
with zipfile.ZipFile(path_zip, 'r') as zip_ref:
    zip_ref.extractall(tempdir)

# Listar arquivos extraídos
path_sinistros = [
    os.path.join(tempdir, f) for f in os.listdir(tempdir) if f.startswith("sinistros")
]

# Ler os dados
sinistros_01 = pd.read_csv(
    path_sinistros[0],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

sinistros_02 = pd.read_csv(
    path_sinistros[1],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

sinistros = pd.concat([sinistros_01, sinistros_02])

# Remover diretório temporário
shutil.rmtree(tempdir)

# Ler demais arquivos
name_municipio = "tb_municipio.csv"
path_municipio = (
    importlib.resources
    .files("infosiga_py")
    .joinpath(raw_folder, name_municipio)
)

df_municipios = pd.read_csv(path_municipio)

name_ibge = "RELATORIO_DTB_BRASIL_MUNICIPIO.xls"
path_ibge = (
    importlib.resources
    .files("infosiga_py")
    .joinpath(raw_folder, name_ibge)
)

df_ibge = pd.read_excel(path_ibge, skiprows=6)

df_ibge_sp = (
    df_ibge
    .clean_names()
    .query("nome_uf == 'São Paulo'")[
        ["codigo_municipio_completo", "nome_municipio"]
    ]
    .rename(columns={"codigo_municipio_completo":"cod_ibge"})
)

# Processar os dados
sinistros["tipo_registro"] = sinistros["tipo_registro"].replace({
    "SINISTRO FATAL": "Sinistro fatal",
    "SINISTRO NAO FATAL": "Sinistro não fatal",
    "NOTIFICACAO": "Notificação"
})

sinistros["data_sinistro"] = pd.to_datetime(
    sinistros["data_sinistro"],
    dayfirst=True
)

sinistros["numero_logradouro"] = pd.to_numeric(
    sinistros["numero_logradouro"],
    errors='coerce'
)

sinistros["tipo_via"] = sinistros["tipo_via"].replace({
    "NAO DISPONIVEL": None,
    "RODOVIAS": "Rodovias",
    "RURAL": "Rodovias",
    "RURAL (COM CARACTERÍSTICA DE URBANA)": "Rodovias",
    "URBANA": "Vias municipais",
    "VIAS MUNICIPAIS": "Vias municipais"
})

# Validando as coordenadas

sinistros["longitude"] = (
    sinistros["longitude"]
    .str.replace(",", ".", regex=True)
    .astype(float)
)

sinistros.loc[
    (sinistros["longitude"] > -44.1) | (sinistros["longitude"] < -53.1),
    "longitude"
] = None

sinistros["latitude"] = (
    sinistros["latitude"]
    .str.replace(",", ".", regex=True)
    .astype(float)
)

sinistros.loc[
    (sinistros["latitude"] > -19.7) | (sinistros["latitude"] < -25.3),
    "latitude"
] = None

cols_tp_veiculo = [
    col for col in sinistros.columns if col.startswith("tp_veiculo")
]

sinistros[cols_tp_veiculo] = sinistros[cols_tp_veiculo].fillna(0)

cols_gravidade = [
    col for col in sinistros.columns if col.startswith("gravidade")
]

sinistros[cols_gravidade] = sinistros[cols_gravidade].fillna(0)

sinistros["administracao_via"] = sinistros["administracao"].replace({
    "CONCESSIONÁRIA": "Concessionária",
    "CONCESSIONÁRIA-ANTT": "Concessionária",
    "CONCESSIONÁRIA-ARTESP": "Concessionária",
    "NAO DISPONIVEL": None,
    "PREFEITURA": "Prefeitura"
}).fillna(sinistros["administracao"])

sinistros["jurisdicao_via"] = sinistros["jurisdicao"].replace({
    "ESTADUAL": "Estadual",
    "MUNICIPAL": "Municipal",
    "FEDERAL": "Federal",
    "NAO DISPONIVEL": None
})

sinistros["tipo_sinistro_primario"] = sinistros["tipo_acidente_primario"].replace({
    "ATROPELAMENTO": "Atropelamento",
    "COLISAO": "Colisão",
    "CHOQUE": "Choque",
    "NAO DISPONIVEL": None
})

cols_tp_sinistro = [
    col for col in sinistros.columns if col.startswith("tp_sinistro")
]

for col in cols_tp_sinistro:
    sinistros[col] = sinistros[col].map({"S": 1, None: 0})

sinistros = sinistros.query(
    "tipo_registro in ['Sinistro fatal', 'Sinistro não fatal']"
)

# Junções
df_final = sinistros.merge(
    df_municipios,
    left_on="municipio", 
    right_on="s_ds_municipio", 
    how="left"
)

df_final["cod_ibge"] = df_final["cod_ibge"].astype(str)
df_ibge_sp["cod_ibge"] = df_ibge_sp["cod_ibge"].astype(str)

df_final = df_final.merge(df_ibge_sp, on="cod_ibge", how="left")

# Seleção de colunas
df_final = df_final[[
    "id_sinistro",
    "data_sinistro", 
    "hora_sinistro", 
    "cod_ibge",
    "nome_municipio",
    "logradouro", 
    "numero_logradouro", 
    "tipo_via", 
    "longitude", 
    "latitude"
] + cols_tp_veiculo + ["tipo_registro"] + cols_gravidade + [
    "administracao_via",
    "jurisdicao_via", 
    "tipo_sinistro_primario"
] + cols_tp_sinistro]

# Salvar em formato Parquet

export_path = importlib.resources.files("infosiga_py").joinpath("data")
export_parquet_path = export_path.joinpath("infosiga_sinistros.parquet")

df_final.to_parquet(export_parquet_path, index=False)