# Local candidate workflow

Use targeted tests and linters during short development iterations. Before
installing a candidate on the host, all applicable local tests and linters must
pass. The five project gates remain mandatory; prior results may be reused only
for byte-identical relevant inputs, with targeted checks for any isolated later
change.

Install an exact committed branch candidate through a reviewed transactional
entry point that retains the service-stop, migration, rollback, and storage
integrity checks. Verify the live service and pendant behavior, then use that
feedback to accept or revise the change. The installed candidate is not a
GitHub release or tag.

Remote CI, PR, security review, merge, and release are outside the local
installation hot path. Push the accepted change after live feedback, complete
the existing remote gates before protected-main merge, and let the GitHub
release follow in the background.
