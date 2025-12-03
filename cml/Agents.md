# Instructions
- Cannot upgrade the Python Version
- Cannot uninstall any python package that has been preinstalled in this environmnet.
- Any package installed via 'requirements.txt' can be uninstalled, downgraded / upgraded.
- No sudo access, cannot install OS libraries. Use Python packages or pre built executables as far as possible.
- This environment provides MLFlow here for use via env variables `MLFLOW_REGISTRY_URI` and `MLFLOW_TRACKING_URI`
- API endpoint for CML is `CDSW_API_URL`
- CML API Key v2 is `CDSW_APIV2_KEY`
- CML API Key v1 is `CDSW_API_KEY`
- CDSW Project ID is `CDSW_PROJECT_ID` 
- CML API documentation is here - https://https://ml-fa07e58a-e58.maybank1.xfaz-gdb4.cloudera.site/api/v2/python/
- CML API Swagger json is here - https://ml-fa07e58a-e58.maybank1.xfaz-gdb4.cloudera.site/api/v2/swagger.json
- Prompt for any configs that you are unable to find.
- App Launcher must use:
  - Bind to `127.0.0.1`
  - Use `CDSW_APP_PORT` (or `PORT`, else 8080)
- Do not add OS-level dependencies; rely on Python packages in `requirements.txt`.


Sample CDSW Application code looks like below - 
```
import os
import time

import h2o


def main() -> None:
    """Launch an H2O cluster bound to CDSW's application port."""
    port = int(os.environ.get("CDSW_APP_PORT", "10000"))

    # Bind the cluster to localhost so CDSW can proxy it safely.
    h2o.init(ip="127.0.0.1", port=port, strict_version_check=False)

    # Keep the process alive so the web UI stays accessible.
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        h2o.shutdown(prompt=False)


if __name__ == "__main__":
    main()
```
