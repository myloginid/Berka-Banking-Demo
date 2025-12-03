# Cloudera AI Workbench SSH Endpoint (Project: manishm/bankdemo)

This guide documents how to create and use a local SSH endpoint into a Cloudera AI Workbench session for project `manishm/bankdemo`, along with runtime selection, Python version, installed packages, and the forwarded SSH port. Values like project, runtime, resources, URL, and port are environment-specific and should be adjusted as needed.

## Variables (update for your environment)

- PROJECT: `manishm/bankdemo`
- USERNAME: your Cloudera AI username (from the UI)
- URL: your Cloudera AI Workbench URL (for example, `https://ml-<cluster>`)
- APIKEY: your Legacy API Key (User Settings > API Keys)
- RUNTIME_ID: an ML Runtime ID compatible with your needs (Python 3.10 here)
- CPU: `2`
- MEM: `8`
- PORT: `9750`

## Prerequisites

- `cdswctl` CLI available locally (download from Cloudera AI UI).
- Your SSH public key is added in the Cloudera AI UI (User Settings > Remote Editing).
- Config file at `$HOME/.cdsw/config.yaml` or symlinked to your workspace copy.
- macOS only: remove quarantine from newly downloaded binaries.

## Steps

1) Prepare config

Create or update `$HOME/.cdsw/config.yaml`:

```yaml
username: <USERNAME>
url: <URL>
auth:
  authtype: 1
  basic: null
  apikey: <APIKEY>
```

2) macOS quarantine (if applicable)

If the binary was downloaded via browser, remove quarantine:

```bash
xattr -d com.apple.quarantine ./cdswctl
# or define an alias and use it
alias unquarantine='xattr -d com.apple.quarantine'
unquarantine ./cdswctl
```

3) Login (once per machine or when keys rotate)

```bash
./cdswctl login -n "$USERNAME" -u "$URL" -y "$APIKEY"
```

4) Confirm project and engine type

```bash
./cdswctl projects list
./cdswctl projects getEngineType -p "$PROJECT"    # expect: ml_runtime
```

5) Pick a Python 3.10 Runtime ID

List available runtimes and filter for Python 3.10. Example with `jq`:

```bash
./cdswctl runtimes list | jq -r '.runtimes[] | select((.editor|test("Workbench")) and (.kernel|test("Python 3.10"))) | "\(.id)\t\(.editor)\t\(.kernel)\t\(.imageIdentifier)"'
```

For this run, we used `RUNTIME_ID=618` (Workbench, Python 3.10).

6) Start the SSH endpoint

```bash
./cdswctl ssh-endpoint \
  -p "$PROJECT" \
  -r "$RUNTIME_ID" \
  -c "$CPU" \
  -m "$MEM" \
  --port "$PORT"
```

The command will print the SSH line to use, e.g.:

```bash
ssh -p 9750 cdsw@localhost
```

7) Connect and inspect Python

```bash
ssh -p "$PORT" cdsw@localhost
# inside the session
whoami
python3 --version
pip --version
pip list
```

8) Stop the endpoint (when finished)

```bash
# Using pattern with the chosen port
pkill -f "cdswctl ssh-endpoint .* --port $PORT" || true
```

## Runtime Types (what you may see)

- Workbench: Terminal-first experience with Python kernels (standard or CUDA images).
- PBJ Workbench: Cloudera PBJ Workbench variants (Python kernels, standard or CUDA).
- JupyterLab: JupyterLab editor with Python kernels (standard or CUDA).
- RStudio: RStudio editor with R kernels.
- VsCode: VS Code-in-container editor variants.
- Zeppelin: Apache Zeppelin-based runtime.

Notes
- Variants often include “standard” and “cuda” images.
- Available runtimes are workspace-specific and may include official and custom images.
- Use `./cdswctl runtimes list` to enumerate and pick the best fit.

## Current Run Details (documented)

- Project: `manishm/bankdemo`
- Runtime used: `id=95`, `editor=PBJ Workbench`, `kernel=Python 3.10`, `edition=Standard`
- Resources: `CPU=2`, `MEM=8`
- SSH port forwarded: example `9810`
- Python:
  - Version: `Python 3.10.x` (PBJ Workbench runtime)
  - Interpreter: `/usr/local/bin/python3`
  - Pip: `pip 25.1.1` (Python 3.10)

Installed Python packages (pip list from runtime 95)

```
Package                            Version
---------------------------------- ------------
alembic                            1.17.2
anyio                              4.7.0
argon2-cffi                        21.3.0
argon2-cffi-bindings               21.2.0
arrow                              1.2.3
asttokens                          2.2.1
attrs                              24.2.0
backcall                           0.2.0
beautifulsoup4                     4.12.0
bitarray                           2.7.3
bleach                             6.0.0
blinker                            1.9.0
boto3                              1.34.149
botocore                           1.34.162
cachetools                         5.5.0
certifi                            2024.7.4
cffi                               1.16.0
charset-normalizer                 3.3.2
click                              8.1.7
cloudpickle                        3.1.0
cml                                1.0.0
cmlapi                             25.10.31
comm                               0.2.2
contourpy                          1.0.7
cryptography                       44.0.1
cycler                             0.11.0
databricks-sdk                     0.39.0
debugpy                            1.8.9
decorator                          5.1.1
defusedxml                         0.7.1
Deprecated                         1.2.15
docker                             7.1.0
entrypoints                        0.4
exceptiongroup                     1.2.2
executing                          1.2.0
fastjsonschema                     2.16.3
filelock                           3.19.1
Flask                              3.1.2
fonttools                          4.43.0
fqdn                               1.5.1
fsspec                             2025.9.0
gitdb                              4.0.11
GitPython                          3.1.43
google-auth                        2.37.0
graphene                           3.4.3
graphql-core                       3.2.7
graphql-relay                      3.2.0
greenlet                           3.2.4
gssapi                             1.8.3
gunicorn                           23.0.0
huey                               2.5.4
idna                               3.7
importlib_metadata                 8.5.0
impyla                             0.21.0
ipykernel                          6.29.5
ipython                            8.10.0
ipython-genutils                   0.2.0
isoduration                        20.11.0
itsdangerous                       2.2.0
jedi                               0.19.1
Jinja2                             3.1.6
jmespath                           1.0.1
joblib                             1.5.2
jsonpointer                        2.3
jsonschema                         4.22.0
jsonschema-specifications          2023.12.1
jupyter_client                     7.4.9
jupyter_core                       5.8.1
jupyter-events                     0.10.0
jupyter-kernel-gateway             2.5.2
jupyter_server                     2.14.1
jupyter_server_terminals           0.5.3
jupyterlab-pygments                0.2.2
kerberos                           1.3.1
kiwisolver                         1.4.4
krb5                               0.5.1
Mako                               1.3.10
Markdown                           3.10
MarkupSafe                         2.1.2
matplotlib                         3.7.0
matplotlib-inline                  0.1.6
mistune                            2.0.5
mlflow                             2.19.0
mlflow-CML-plugin                  0.0.1
mlflow-skinny                      2.19.0
mpmath                             1.3.0
nbclassic                          0.5.5
nbclient                           0.7.3
nbconvert                          7.3.0
nbformat                           5.8.0
nest-asyncio                       1.5.6
networkx                           3.3
notebook                           6.5.3
notebook_shim                      0.2.2
numpy                              1.26.4
opentelemetry-api                  1.29.0
opentelemetry-sdk                  1.29.0
opentelemetry-semantic-conventions 0.50b0
overrides                          7.7.0
packaging                          23.2
pandas                             2.1.4
pandocfilters                      1.5.0
parso                              0.8.3
pexpect                            4.8.0
pickleshare                        0.7.5
pillow                             10.3.0
pip                                25.1.1
platformdirs                       4.3.6
prometheus-client                  0.16.0
prompt-toolkit                     3.0.38
protobuf                           4.25.3
psutil                             6.1.0
ptyprocess                         0.7.0
pure-eval                          0.2.2
pure-sasl                          0.6.2
py4j                               0.10.9.7
pyarrow                            18.1.0
pyasn1                             0.6.1
pyasn1_modules                     0.4.1
pycparser                          2.22
Pygments                           2.15.1
pyparsing                          3.0.9
pyspark                            3.5.1
pyspnego                           0.11.0
python-dateutil                    2.8.2
python-dotenv                      1.2.1
python-json-logger                 2.0.7
pytz                               2023.3.post1
PyYAML                             6.0
pyzmq                              26.2.0
raz_client                         1.1.0
referencing                        0.35.1
requests                           2.32.3
requests-kerberos                  0.15.0
rfc3339-validator                  0.1.4
rfc3986-validator                  0.1.1
rpds-py                            0.18.1
rsa                                4.9
s3transfer                         0.10.2
scikit-learn                       1.7.2
scipy                              1.15.3
Send2Trash                         1.8.3
setuptools                         80.3.1
six                                1.16.0
smmap                              5.0.1
sniffio                            1.3.0
soupsieve                          2.4
SQLAlchemy                         2.0.44
sqlparse                           0.5.3
stack-data                         0.6.2
sympy                              1.13.1
terminado                          0.17.1
threadpoolctl                      3.6.0
thrift                             0.16.0
thrift-sasl                        0.4.3
tinycss2                           1.2.1
tomli                              2.3.0
torch                              2.5.1+cpu
tornado                            6.5
traitlets                          5.9.0
trino                              0.330.0
typing_extensions                  4.15.0
tzdata                             2023.3
tzlocal                            5.2
uri-template                       1.2.0
urllib3                            2.2.2
wcwidth                            0.2.6
webcolors                          1.13
webencodings                       0.5.1
websocket-client                   1.8.0
Werkzeug                           3.1.4
wheel                              0.45.1
wrapt                              1.17.0
zipp                               3.21.0
```

## Enabling Spark in CML sessions (runtime add‑ons)

In the CML UI you can toggle “Enable Spark” on a runtime (for example, PBJ Workbench / Python 3.10 / Spark 3.5.1 – CDE 1.24.1 – HOTFIX‑1).  
From `cdswctl` you achieve the same by attaching the corresponding **runtime add‑on**.

1) Discover available Spark add‑ons

```bash
./cdswctl runtime-addons list
```

On the Maybank environment this returns entries like:

- `id: 3, component: Spark, displayName: "Spark 3.5.1 - CDE 1.24.1 - HOTFIX-1"`

2) Start an ssh-endpoint with Spark enabled

Example for project `manishm/bankdemo`, PBJ Workbench Python 3.10 Standard (`runtime-id=95`), Spark 3.5.1 add‑on (`id=3`), 2 vCPU / 8 GiB, port 9750:

```bash
./cdswctl ssh-endpoint \
  -p manishm/bankdemo \
  -r 95 \
  -c 2 \
  -m 8 \
  --port 9750 \
  --addons 3

ssh -p 9750 cdsw@localhost
```

Any CML session started with `--addons <spark-addon-id>` will show “Enable Spark” turned on in the UI and can use the configured Spark on the attached CDP cluster.

## Available runtimes (Maybank workspace)

The following ML Runtimes are currently available in the `manishm/bankdemo` workspace (`./cdswctl runtimes list`):

- 80  – PBJ Workbench, Cloudera Data Visualization, 8.0.6 (Data Visualization)
- 81  – JupyterLab, Conda, 2025.09 (Tech Preview)
- 82  – JupyterLab, Python 3.10, 2025.09 (Nvidia GPU)
- 83  – JupyterLab, Python 3.10, 2025.09 (Standard)
- 84  – JupyterLab, Python 3.11, 2025.09 (Nvidia GPU)
- 85  – JupyterLab, Python 3.11, 2025.09 (Freshline)
- 86  – JupyterLab, Python 3.11, 2025.09 (Standard)
- 87  – JupyterLab, Python 3.12, 2025.09 (Nvidia GPU)
- 88  – JupyterLab, Python 3.12, 2025.09 (Standard)
- 89  – JupyterLab, Python 3.13, 2025.09 (Nvidia GPU)
- 90  – JupyterLab, Python 3.13, 2025.09 (Standard)
- 91  – JupyterLab, Python 3.9, 2025.09 (Nvidia GPU)
- 92  – JupyterLab, Python 3.9, 2025.09 (Standard)
- 93  – JupyterLab, R 4.5, 2025.09 (Freshline)
- 94  – PBJ Workbench, Python 3.10, 2025.09 (Nvidia GPU)
- 95  – PBJ Workbench, Python 3.10, 2025.09 (Standard)
- 96  – PBJ Workbench, Python 3.11, 2025.09 (Nvidia GPU)
- 97  – PBJ Workbench, Python 3.11, 2025.09 (Standard)
- 98  – PBJ Workbench, Python 3.12, 2025.09 (Nvidia GPU)
- 99  – PBJ Workbench, Python 3.12, 2025.09 (Standard)
- 100 – PBJ Workbench, Python 3.13, 2025.09 (Nvidia GPU)
- 101 – PBJ Workbench, Python 3.13, 2025.09 (Standard)
- 102 – PBJ Workbench, Python 3.9, 2025.09 (Nvidia GPU)
- 103 – PBJ Workbench, Python 3.9, 2025.09 (Standard)
- 104 – PBJ Workbench, R 4.5, 2025.09 (Standard)
- 105 – PBJ Workbench, Scala 2.12, 2025.09 (Standard)
- 106 – PBJ Workbench, Cloudera Data Visualization, 8.0.8 (Data Visualization)

For the Berka demo scripts we typically use:

- Runtime **95** – PBJ Workbench, Python 3.10, Standard.

## Available runtime add‑ons

`./cdswctl runtime-addons list` returns these add‑ons in this environment:

- 5 – Hadoop CLI – CDP 7.2.18.1000
- 4 – Hadoop CLI – CDP 7.3.1.101
- 6 – Spark 2.4.8 – CDE 1.24.1 – HOTFIX-1
- 1 – Spark 3.2.3 – CDE 1.24.1 – HOTFIX-1
- 2 – Spark 3.3.0 – CDE 1.24.1 – HOTFIX-1
- 3 – Spark 3.5.1 – CDE 1.24.1 – HOTFIX-1

For Spark 3.5.1 with PBJ Workbench Python 3.10, use:

- Runtime `-r 95` and add‑on `--addons 3`.

## Troubleshooting

- If `./cdswctl` exits with “Killed: 9” on macOS, clear quarantine with `xattr -d com.apple.quarantine ./cdswctl`.
- If SSH asks for a passphrase, use the passphrase for the SSH key you added to the UI.
- If the port is busy, pick a different `PORT` with `--port` and update the SSH command accordingly.
- If the project uses ML Runtimes, `-r <RUNTIME_ID>` is required; omit `-r` for legacy engine projects.
