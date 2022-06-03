import click

from merak.client import Client


@click.group(hidden=True)
def delete():
    """
    Delete commands that refer to mutations of merak API metadata.

    \b
    Usage:
        $ merak delete [OBJECT]

    \b
    Arguments:
        project    Delete projects

    \b
    Examples:
        $ merak delete project "Goodbye, World!"
        Goodbye, World! deleted

    """


@delete.command(hidden=True)
@click.argument("name", required=True)
def project(name):
    """
    Delete projects with the merak API that organize flows.

    \b
    Arguments:
        name                TEXT    The name of a project to delete     [required]

    """
    try:
        Client().delete_project(project_name=name)
    except ValueError as exc:
        click.echo(f"{type(exc).__name__}: {exc}")
        click.secho("Error deleting project", fg="red")
        return

    click.secho("{} deleted".format(name), fg="green")
