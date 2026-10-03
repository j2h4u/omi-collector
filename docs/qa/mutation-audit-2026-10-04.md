# Mutation audit record — 2026-10-04

The original native campaign completed with exit status 0 against commit
`545bbe71aaf19e38edcfc17d266747f275740b6f`. It generated 5,707 mutants across
66 source files (60 mutated): 3,786 were zapped, 1,632 survived, 91 timed out,
and 198 errored. The campaign is `complete_unresolved`; exit status 0 does not
resolve its errors or timeouts and does not certify the full audit.

Follow-up native runs targeted selected files and their designated tests. Their
receipts reconcile 133 original survivors killed in those scopes. Sixteen
corrections kills used a pre-final fixture identity and are retained with that
caveat. These scoped results do not establish a new full-project survivor
total; the reconciliation check, for example, matched all 67 original
survivors for one source and killed 3 in its two-test scope.

The source map covers all 66 source files, with 60 mutated and 55 selected test
files. The evidence bundle is stored at
`/srv/omi-collector-mutation-audit/20261004/mutation-audit-20261004.tar.gz`;
its manifest records the original archive checksum and hashes for the compact
receipts, source map, and runtime-control evidence. The original native cache
and report are preserved in the bundle. This record does not claim a clean
full-project mutation audit.

Final repository gates are recorded after running against the integrated
committed tree. The Docker build already passed on unchanged production
sources; it is not repeated for this test-only integration.
