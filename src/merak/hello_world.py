"""
A very basic 'hello-world' flow for getting started quickly
Examples:
    Run this flow locally
    --------------------
    >>> from merak.hello_world import hello_flow
    >>> hello_flow.run()
    Run this flow with a different parameter than the default
    --------------------------------------------------------
    >>> from merak.hello_world import hello_flow
    >>> hello_flow.run(name="marvin")
    Register this flow with the merak backend
    ------------------------------------------
    $ merak create project 'default'
    $ merak register --project default -m merak.hello_world
    Run this flow with the merak backend and agent
    ------------------------------------------
    $ merak run --name "hello-world" --watch
"""

from merak import task, Flow, Parameter


@task(log_stdout=True)
def say_hello(to: str) -> None:
    print(f"Hello {to}")


@task()
def capitalize(word: str) -> str:
    return word.capitalize()


with Flow("hello-world") as hello_flow:
    name = Parameter("name", default="world")
    say_hello(capitalize(name))
