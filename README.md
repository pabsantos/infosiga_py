# infosiga_py

The goal of infosiga_py is to provide three datasets related to road crashes that happened in the State of São Paulo, Brazil, from the Infosiga.SP data repository. These data includes attributes from the crashes (sinistros), victims (vitimas) and vehicles (veiculos).

## Installation

infosiga_py can be installed with:

```
pip install git+https://github.com/pabsantos/infosiga_py.git
```

## Usage

`load_infosiga()` can be used to load one of the three datasets:

```python
from infosiga_py import load_infosiga

vitimas = load_infosiga("vitimas")
```