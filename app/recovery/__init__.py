"""
app/recovery — stale-worker reclamation, failed-step remediation, drift reconciliation.

Modules
-------
reclaim.py      WorkerReclaimer: recovers RUNNING rows with stale heartbeats and
                QUEUED rows that were never claimed (T-5.1).
remediation.py  RemediationWorker: exponential-backoff retry queue for failed
                workflow steps, with escalation to NEEDS_ATTENTION (T-5.2).
reconciler.py   Reconciler: read-only drift sweep between MSSQL and F5/Infoblox.
                Report-only. Write mode is permanently disabled (D-10).
"""
