import subprocess
import sys
import textwrap
import unittest


# Run SIGINT in a child process so it cannot interrupt the test runner.
SHUTDOWN_SCRIPT = textwrap.dedent("""\
    import asyncio
    import gc
    import signal
    import sys
    from unittest.mock import patch

    import aiohttp
    import hypertrader

    backend, scenario = sys.argv[1:]
    if backend == "uvloop":
        import uvloop
        uvloop.install()

    loops = []
    sessions = []
    tasks = []
    events = []
    errors = []

    async def background(attempt):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            events.append((attempt, "background closed"))

    async def application(argv):
        attempt = len(loops)
        loop = asyncio.get_running_loop()
        loops.append(loop)
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        session = aiohttp.ClientSession()
        sessions.append(session)
        tasks.append(asyncio.create_task(background(attempt)))
        try:
            await asyncio.sleep(0)
            if scenario.startswith("restart") and attempt == 0:
                raise RuntimeError("trigger restart")
            if scenario.endswith("interrupt"):
                loop.call_soon(signal.raise_signal, signal.SIGINT)
                await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            await session.close()
            events.append((attempt, "session closed"))

    with patch.object(hypertrader, "async_main", application), patch.object(
        hypertrader, "configure_runtime_logging", return_value="unused.log"
    ):
        hypertrader.main([])

    gc.collect()
    assert len(loops) == (2 if scenario.startswith("restart") else 1), events
    assert all(loop.is_closed() for loop in loops), "event loop left open"
    assert all(session.closed for session in sessions), "HTTP session left open"
    assert all(task.done() for task in tasks), "background task left pending"
    for attempt in range(len(loops)):
        assert (attempt, "session closed") in events, events
        assert (attempt, "background closed") in events, events
    assert not errors, errors
    print("shutdown verified")
""")


class CliShutdownTests(unittest.TestCase):
    def test_shutdown_and_restart_release_async_resources(self):
        for backend in ("asyncio", "uvloop"):
            for scenario in ("normal", "interrupt", "restart", "restart_interrupt"):
                with self.subTest(backend=backend, scenario=scenario):
                    result = subprocess.run(
                        [sys.executable, "-c", SHUTDOWN_SCRIPT, backend, scenario],
                        capture_output=True, text=True, timeout=20,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn("shutdown verified", result.stdout)
                    self.assertNotIn("Unclosed", result.stderr)
                    self.assertNotIn("Task was destroyed", result.stderr)
