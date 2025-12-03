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
- Runtime used: `id=618`, `editor=Workbench`, `kernel=Python 3.10`, `image=docker.io/syedfrm/my-cloudera-ml-runtime:latest`
- Resources: `CPU=2`, `MEM=8`
- SSH port forwarded: `9750`
- Python:
  - Version: `Python 3.10.14`
  - Interpreter: `/usr/local/bin/python3`
  - Pip: `pip 24.2` (Python 3.10)

Installed Python packages (pip list)

```
Package                            Version
---------------------------------- ------------
asttokens                          2.2.1
backcall                           0.2.0
bitarray                           2.7.3
boto3                              1.34.149
botocore                           1.34.162
cachetools                         5.5.0
cdsw                               1.1.0
certifi                            2024.7.4
cffi                               1.16.0
charset-normalizer                 2.0.6
click                              8.1.7
cloudpickle                        3.1.0
cml                                1.0.0
cmlapi                             25.8.21
comm                               0.1.3
contourpy                          1.0.7
cryptography                       42.0.8
cycler                             0.11.0
databricks-sdk                     0.39.0
debugpy                            1.6.7
decorator                          5.1.1
Deprecated                         1.2.15
entrypoints                        0.4
executing                          1.2.0
fonttools                          4.43.0
gitdb                              4.0.11
GitPython                          3.1.43
google-auth                        2.37.0
gssapi                             1.8.3
idna                               3.7
importlib_metadata                 8.5.0
impyla                             0.18.0
ipykernel                          6.22.0
ipython                            8.10.0
jedi                               0.19.1
jmespath                           1.0.1
jupyter_client                     7.4.4
jupyter_core                       5.3.0
kerberos                           1.3.1
kiwisolver                         1.4.4
krb5                               0.5.1
matplotlib                         3.7.0
matplotlib-inline                  0.1.3
mlflow-CML-plugin                  0.0.1
mlflow-skinny                      2.19.0
nest-asyncio                       1.5.5
numpy                              1.26.4
opentelemetry-api                  1.29.0
opentelemetry-sdk                  1.29.0
opentelemetry-semantic-conventions 0.50b0
packaging                          23.2
pandas                             2.1.4
parso                              0.8.3
pexpect                            4.8.0
pickleshare                        0.7.5
pillow                             10.3.0
pip                                24.2
platformdirs                       3.5.0
prompt-toolkit                     3.0.38
protobuf                           4.25.3
psutil                             5.9.5
ptyprocess                         0.6.0
pure-eval                          0.2.2
pure-sasl                          0.6.2
py4j                               0.10.9.5
pyasn1                             0.6.1
pyasn1_modules                     0.4.1
pycparser                          2.22
Pygments                           2.15.1
pyparsing                          3.0.9
pyspnego                           0.11.0
python-dateutil                    2.8.2
pytz                               2023.3.post1
PyYAML                             6.0.2
pyzmq                              25.1.2
raz_client                         1.1.0
requests                           2.31.0
requests-kerberos                  0.15.0
rsa                                4.9
s3transfer                         0.10.2
setuptools                         74.1.2
six                                1.16.0
smmap                              5.0.1
sqlparse                           0.5.3
stack-data                         0.6.2
thrift                             0.16.0
thrift-sasl                        0.4.3
tornado                            6.3.3
traitlets                          5.9.0
trino                              0.330.0
typing_extensions                  4.10.0
tzdata                             2023.3
tzlocal                            5.2
urllib3                            1.26.18
wcwidth                            0.2.5
wheel                              0.44.0
wrapt                              1.17.0
zipp                               3.21.0
```

## Troubleshooting

- If `./cdswctl` exits with “Killed: 9” on macOS, clear quarantine with `xattr -d com.apple.quarantine ./cdswctl`.
- If SSH asks for a passphrase, use the passphrase for the SSH key you added to the UI.
- If the port is busy, pick a different `PORT` with `--port` and update the SSH command accordingly.
- If the project uses ML Runtimes, `-r <RUNTIME_ID>` is required; omit `-r` for legacy engine projects.

Sample Code
``` 
./cdswctl ssh-endpoint -p manishm/bank_rm_demo -r 693 -c 2 -m 8 --port 9760                            
```




  