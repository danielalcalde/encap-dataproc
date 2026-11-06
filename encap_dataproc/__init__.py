"""Lightweight registration: no Google imports or authentication during discovery."""

from encap_lib.backends import CLIOption, Registration
from encap_dataproc.instances import parse_instances

__version__ = "0.1.4"


def _factory(config):
    from encap_dataproc.backend import DataprocBackend
    return DataprocBackend(config)


def _list_runs(config):
    from encap_dataproc.backend import DataprocBackend
    return DataprocBackend.list_runs(config)


registration = Registration(
    name="dataproc", factory=_factory, selectors=("-gcp",), api_version=3, plugin_version=__version__,
    cli_options=(CLIOption(
        flags=("-gcp_i",), key="i", parse=parse_instances, metavar="INSTANCES",
        help="Launch Dataproc instances: a count, list of IDs, or range(...)",
    ),),
    supports_dependencies=True,
    supports_rerun=True,
    supports_autopull=True,
    supports_ssh=True,
    list_runs=_list_runs,
)
