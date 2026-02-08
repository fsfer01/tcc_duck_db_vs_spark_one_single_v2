Nesse código, estou realizando comparações entre o duckdb vs pyspark em um ambiente local.


no terminal:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install jupyter ipykernel
python -m ipykernel install --user --name=venv --display-name "Python (venv local)"

```

depois disso, só usar o kernel Python (venv)" ao rodar o jupyter
