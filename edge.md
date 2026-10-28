# Local candidate workflow

Use targeted tests and linters during short development iterations. The full
local contract is `just verify`: static checks, the complete pytest suite with
the CRAP threshold, the locked dependency audit, and the Docker runtime smoke
(which includes the image build). Do not rerun the standalone unit or Docker
build recipes as part of that full gate. Before installing a candidate on the
host, `just verify` must pass; prior results may be reused only for byte-identical
relevant inputs, with targeted checks for any isolated later change.

Install an exact committed branch candidate through a reviewed transactional
entry point that retains the service-stop, migration, rollback, and storage
integrity checks. Verify the live service and pendant behavior, then use that
feedback to accept or revise the change. The installed candidate is not a
GitHub release or tag.

Remote CI, PR, security review, merge, and release are outside the local
installation hot path. Push the accepted change after live feedback, complete
the existing remote gates before protected-main merge, and let the GitHub
release follow in the background.
