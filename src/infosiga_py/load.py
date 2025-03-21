import importlib.resources
import pandas as pd

def load_infosiga(file: str) -> pd.DataFrame:
    """
    Load a specific infosiga dataset as a pandas DataFrame.
    Parameters:
    -----------
    file : str
        The name of the dataset to load. Must be one of the following:
        - "veiculos": Dataset containing vehicle information.
        - "vitimas": Dataset containing victim information.
        - "sinistros": Dataset containing road crash information.
    Returns:
    --------
    pd.DataFrame
        A pandas DataFrame containing the data from the specified dataset.
    Raises:
    -------
    ValueError
        If the provided file name is not one of the valid options.
    Notes:
    ------
    The function expects the dataset to be stored as a Parquet file in the
    "data" directory within the "infosiga_py" package. The file name is
    expected to follow the pattern "infosiga_<file>.parquet".
    """
    valid_files = ["veiculos", "vitimas", "sinistros"]
    if file not in valid_files:
        raise ValueError(f"file must be one of {valid_files}")
    
    file_name = f"infosiga_{file}.parquet"
    path = importlib.resources.files("infosiga_py").joinpath("data", file_name)

    data = pd.read_parquet(path)
    return data