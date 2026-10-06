## Cluster loss and requeue

`Emr.Runner.runJobs` replaces a cluster that dies for a recoverable reason
(`INSTANCE_FAILURE`, `INTERNAL_ERROR`, `INSTANCE_FLEET_TIMEOUT` — e.g. a Spot
reclaim or hardware failure) and resubmits that cluster's unfinished steps to
the replacement. Steps that already completed are not re-run. Up to
`maxReplacements` (default 5) replacements are made per `runJobs` call;
`replacementDef` (default identity) can alter the `ClusterDef` used for the
replacement, for example to fall back from Spot to on-demand.

A step that fails, or a cluster that dies for any other reason, still
terminates every cluster and throws, as before. `runJobs` also throws if
fewer steps were reported `COMPLETED` than were submitted.

Steps must therefore be safe to run twice: a step killed mid-run may have
written part of its output, and the re-run overwrites it. Steps that upload
fixed file names with `aws s3 cp` or Spark `mode('overwrite')` are fine.

The requeue happens inside `runJobs`, so a stage's `prepareJob` (which may
clear the stage's whole output prefix) is never re-run.

To exercise the requeue against real EMR, see `src/it/.../EmrRequeueIT.scala`.
