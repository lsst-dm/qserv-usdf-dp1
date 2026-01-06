# qserv-usdf-dp1
Tools and configuration files for ingesting the DP1 catalog in Qserv deployments at USDF and UkDF. The implementation of the ingest workflow
is based on the REST API documented in:
- https://qserv.lsst.io/ingest/api/

## Ingesting into the Kubernetes-based Qserv deployment(s) at UkDF
__IMPORTANT__ Unlike the USDF scenarios, the input data of the catalog must be transferred from SLAC into UKDF and deployed at a specific location from where the data will be served to the ingest workflow by the `nginx` server. The properly configured `nginx` is expected to be a part of the Qserv deployment. All data are packaged into a single archive `dp1.tgz`. The amount of data in the file is slightly less than 200 GB. This document does not provide specific details for locating the file at SLAC and/or transferring it to UKDF. This document only provides instructions on how to correctly unpack the file within Qserv. See further details below.

Log in to the ingest helper pod:
```
kubectl -n qserv-dev exec -it qserv-ingest-0 -c ingest-helper -- bash
```
All subsequent steps are performed within the pod. The first step is to pull the compressed data file and unpack it as shown below:
```
cd /qserv/data/html
mkdir -p dp1/data/
cd dp1/data/
curl <data-file-url> -o dp1.tgz
tar xvf dp1.tgz
rm dp1.tgz
```
Check that the data files are in the right location:
```
ls -l /qserv/data/html/dp1/data/

drwxr-xr-x  2 45386 1126   3 May 21  2025 CcdVisit
drwxr-xr-x  2 45386 1126   5 Jun 30 00:33 CoaddPatches
drwxr-xr-x  2 45386 1126 165 May 20  2025 DiaObject
drwxr-xr-x  2 45386 1126  83 May 23  2025 DiaSource
drwxr-xr-x  2 45386 1126  85 May 20  2025 ForcedSource
drwxr-xr-x  2 45386 1126  83 May 20  2025 ForcedSourceOnDiaObject
drwxr-xr-x  2 45386 1126   4 Jun 27 22:15 MPCORB
drwxr-xr-x  2 45386 1126 168 Jun 16  2025 Object
drwxr-xr-x  2 45386 1126   7 Jun 24 19:24 ObsCore
drwxr-xr-x  2 45386 1126   4 Jun 27 22:19 SSObject
drwxr-xr-x  2 45386 1126   3 May 20  2025 SSSource
drwxr-xr-x  2 45386 1126 172 May 20  2025 Source
drwxr-xr-x  2 45386 1126   3 May 20  2025 Visit
```
Another test would be to pull one of these files via Qserv's `nginx` service:
```
curl http://qserv-ingest-0.qserv-ingest/dp1/data/Visit/Visit.csv -o rubbish.csv
rm rubbish.csv
```
The next step is to get the simple ingest workflow:
```
cd /qserv/data
git clone  https://github.com/lsst-dm/qserv-usdf-dp1.git
cd qserv-usdf-dp1/ingest/qserv-ukdf
```
After that, start the ingest workflow for ``dp1`` by:
```
./ingest_all.sh >& ingest_all.log&
```
Then watch the progress of the ingest by following the log. Normally, it takes about 30 minutes for all the steps of the workflow to be completed.

The next step is to ingest the table `ObsCore` into the catalog `ivoa`:
```
./ingest_Object_ObsCore_in_ivoa.sh
```
The logs of each step of both workflows would be placed into the following folder that is created by the workflow:
```
ls -al logs/
```
The final step would be to tune the scan rating for the tables to the desired values. For example:
```
../tools/set-scan-rating.py --database=dp1 --table=DiaSource 2
../tools/set-scan-rating.py --database=dp1 --table=ForcedSource 25
../tools/set-scan-rating.py --database=dp1 --table=ForcedSourceOnDiaObject 25
../tools/set-scan-rating.py --database=dp1 --table=Source 15
```

## Ingesting into the Kubernetes-based Qserv deployment `qserv-dev-vcluster` at USDF
Follow instructions for obtaining and setting credentials for the cluster:
- https://k8s.slac.stanford.edu/usdf-qserv/commandline

Log in to the ingest helper pod:
```
kubectl -n qserv-dev exec -it qserv-ingest-0 -c ingest-helper -- bash
```
All subsequent steps are performed within the pod:
```
cd /qserv/data
git clone  https://github.com/lsst-dm/qserv-usdf-dp1.git
cd qserv-usdf-dp1/ingest/qserv-dev-vcluster
```
After that, start the ingest workflow for ``dp1`` by:
```
./ingest_all.sh >& ingest_all.log&
```
Then watch the progress of the ingest by following the log. Normally, it takes about 30 minutes for all the steps of the workflow to be completed.

The next step is to ingest the table `ObsCore` into the catalog `ivoa`:
```
./ingest_Object_ObsCore_in_ivoa.sh
```
The logs of each step of both workflows would be placed into the following folder that is created by the workflow:
```
ls -al logs/
```
The final step would be to tune the scan rating for the tables to the desired values. For example:
```
../tools/set-scan-rating.py --database=dp1 --table=DiaSource 2
../tools/set-scan-rating.py --database=dp1 --table=ForcedSource 25
../tools/set-scan-rating.py --database=dp1 --table=ForcedSourceOnDiaObject 25
../tools/set-scan-rating.py --database=dp1 --table=Source 15
```
## Deleting the catalog(s)
These operations should be run from the above-mentioned deployment-specific folder, where the local repository of the Git package is located.
If the folder and the package do not exist, then create them as it was explained earlier.

For UKDF, go to:
```
cd /qserv/data/qserv-usdf-dp1/ingest/qserv-ukdf
```
For USDF, go to:
```
cd /qserv/data/qserv-usdf-dp1/ingest/qserv-dev-vcluster
```
The next step is to generate the configuration file `qserv.json` unless the file already exists. The file will contain the authorization
context for the subsequent operations performed by the ingest tools:
```
source make_config.source
```
../tools/delete-database.py --database=dp1
../tools/delete-database.py --database=ivoa
```
After that, the ingestion of both catalogs can be repeated from scratch.
