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
path_veiculos = [
    os.path.join(tempdir, f) for f in os.listdir(tempdir) if f.startswith("veiculos")
]

# Ler os dados
veiculos_01 = pd.read_csv(
    path_veiculos[0],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

veiculos_02 = pd.read_csv(
    path_veiculos[1],
    encoding="latin1",
    dtype={"latitude": str, "longitude": str},
    sep=';'
)

veiculos = pd.concat([veiculos_01, veiculos_02])

# Remover diretório temporário
shutil.rmtree(tempdir)

cols = ["id_sinistro", "ano_fab", "ano_modelo", "cor_veiculo", "tipo_veiculo"]

infosiga_veiculos = (
    veiculos[cols]
    .rename(columns={"ano_fab": "ano_fabricacao"})
    .assign(
        tipo_veiculo=lambda df: df["tipo_veiculo"].replace({
            "AUTOMOVEL": "Automóvel",
            "MOTOCICLETA": "Motocicleta",
            "CAMINHAO": "Caminhão",
            "ONIBUS": "Ônibus",
            "OUTROS": "Outros",
            "BICICLETA": "Bicicleta",
            "NAO DISPONIVEL": None
        })
    )
)

export_path = importlib.resources.files("infosiga_py").joinpath("data")
export_parquet_path = export_path.joinpath("infosiga_veiculos.parquet")

infosiga_veiculos.to_parquet(export_parquet_path, index=False)
