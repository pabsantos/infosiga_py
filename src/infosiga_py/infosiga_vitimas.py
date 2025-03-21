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
path_pessoas = [
    os.path.join(tempdir, f) for f in os.listdir(tempdir) if f.startswith("pessoas")
]

# Ler os dados
pessoas_01 = pd.read_csv(
    path_pessoas[0],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

pessoas_02 = pd.read_csv(
    path_pessoas[1],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

vitimas = pd.concat([pessoas_01, pessoas_02])

# Remover diretório temporário
shutil.rmtree(tempdir)

vitimas["sexo"] = vitimas["sexo"].replace({
    "MASCULINO": "Masculino",
    "FEMININO": "Feminino",
    "NAO DISPONIVEL": None
})

vitimas["data_obito"] = pd.to_datetime(vitimas["data_obito"], dayfirst=True)

vitimas["tipo_de vítima"] = vitimas["tipo_de vítima"].replace({
    "CONDUTOR": "Condutor",
    "PASSAGEIRO": "Passageiro",
    "PEDESTRE": "Pedestre",
    "NAO DISPONIVEL": None
})

vitimas["tipo_veiculo_vitima"] = vitimas["tipo_veiculo_vitima"].replace({
    "PEDESTRE": "Pedestre",
    "MOTOCICLETA": "Motocicleta",
    "AUTOMOVEL": "Automóvel",
    "NAO DISPONIVEL": None,
    "OUTROS": "Outros",
    "BICICLETA": "Bicicleta",
    "CAMINHAO": "Caminhão",
    "ONIBUS": "Ônibus"
})

vitimas["gravidade_lesao"] = vitimas["gravidade_lesao"].replace({
    "FATAL": "Fatal",
    "NAO DISPONIVEL": None,
    "LEVE": "Leve",
    "GRAVE": "Grave"
})

vitimas["faixa_etaria_demografica"] = vitimas["faixa_etaria_demografica"].replace({
    "NAO DISPONIVEL": None,
    "90 e +": "90+"
})

faixas_etarias = [
    "00 a 04", "05 a 09", "10 a 14", "15 a 19", "20 a 24",
    "25 a 29", "30 a 34", "35 a 39", "40 a 44", "45 a 49",
    "50 a 54", "55 a 59", "60 a 64", "65 a 69", "70 a 74",
    "75 a 79", "80 a 84", "85 a 89", "90+"
]
vitimas["faixa_etaria_demografica"] = pd.Categorical(
    vitimas["faixa_etaria_demografica"],
    categories=faixas_etarias,
    ordered=True
)

vitimas["data_sinistro"] = pd.to_datetime(
    vitimas["data_sinistro"],
    dayfirst=True
)

# Renomear colunas
vitimas = vitimas.rename(columns={
    "tipo_de vítima": "tipo_vitima",
    "faixa_etaria_demografica": "faixa_etaria"
})

# Selecionar colunas finais
infosiga_vitimas = vitimas[[
    "id_sinistro", "data_sinistro", "data_obito", "sexo", "idade", "tipo_vitima",
    "faixa_etaria", "tipo_veiculo_vitima", "gravidade_lesao"
]]


export_path = importlib.resources.files("infosiga_py").joinpath("data")
export_parquet_path = export_path.joinpath("infosiga_vitimas.parquet")

infosiga_vitimas.to_parquet(export_parquet_path, index=False)