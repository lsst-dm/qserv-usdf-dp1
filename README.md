# qserv-usdf-dp1
Tools and configuration files for ingesting the DP1 catalog in Qserv deployments at USDF. The implementation of the ingest workflow
is based on the REST API documented in:
- https://qserv.lsst.io/ingest/api/

## Ingesting into the Kubernetes-based Qserv deployment `qserv-dev-vcluster`
Follow instructions for obtaining and setting credentials for the cluster:
- https://k8s.slac.stanford.edu/usdf-qserv/commandline

Start the ingest pod using the Qserv image from the latest release:
```
kubectl -n qserv-dev run qserv-ingest-0 --image=qserv/lite-build:2025.9.1-rc1
```
Log in to the pod:
```
kubectl -n qserv-dev exec -it qserv-ingest-0 -- bash
```
All subsequent steps are performed within the pod:
```
cd /tmp/
git clone  https://github.com/lsst-dm/qserv-usdf-dp1.git
cd /tmp/qserv-usdf-dp1/ingest/qserv-dev-vcluster
```
Now, create the configuration file using an example found in this folder:
```
mv qserv.json.example qserv.json
```
Edit the file to set the correct values of the authorization keys ``auth-key`` and ``admin-auth-key `` as  are configured in the cluster:
```
cat qserv.json
{
  "instance-id":    "qserv-dev",
  "repl-contr-url": "http://qserv-repl-0.qserv-repls.qserv-dev.svc.cluster.local:25081",
  "auth-key":       "",
  "admin-auth-key": ""
}
```
After that, start the ingest workflow for ``dp1`` by:
```
./ingest_all.sh
```
Normally, it would take about 30 minutes before all the steps of the workflow are completed.

Then run another workflow to ingest tables `ObsCore` into the catalog `ivoa`:
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
These operations should be run from the above-mentioned deployment-specific folder, where the local repo of the Git package was placed
and where the properly configured file `qserv.json` was created. For example:
```
cd /tmp/qserv-usdf-dp1/ingest/qserv-dev-vcluster
../tools/delete-database.py --database=dp1
../tools/delete-database.py --database=ivoa
```
After that, the ingest can be repeated from scratch.
